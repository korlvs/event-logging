package consumer

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	event "github.com/korlvs/event-logging/contracts/event"
	"github.com/korlvs/event-logging/services/event-sink/internal/model"
	"github.com/korlvs/event-logging/services/event-sink/internal/repository"
	"google.golang.org/protobuf/proto"
)

type RestConsumer struct {
	restURL               string
	topic                 string
	group                 string
	username              string
	password              string
	client                *http.Client
	instanceID            string
	repo                  repository.EventRepository
	expectedSchemaIDKey   int
	expectedSchemaIDValue int
}

func NewRestConsumer(
	restURL, topic, group, username, password string,
	repo repository.EventRepository,
	expectedSchemaIDKey, expectedSchemaIDValue int,
) (*RestConsumer, error) {
	c := &RestConsumer{
		restURL:               restURL,
		topic:                 topic,
		group:                 group,
		username:              username,
		password:              password,
		client:                &http.Client{Timeout: 30 * time.Second},
		repo:                  repo,
		expectedSchemaIDKey:   expectedSchemaIDKey,
		expectedSchemaIDValue: expectedSchemaIDValue,
	}
	if err := c.createInstance(); err != nil {
		return nil, err
	}
	if err := c.subscribe(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *RestConsumer) createInstance() error {
	url := fmt.Sprintf("%s/consumers/%s", c.restURL, c.group)
	body := map[string]string{
		"name":              c.group,
		"format":            "binary",
		"auto.offset.reset": "earliest",
	}
	jsonBody, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", url, bytes.NewReader(jsonBody))
	req.Header.Set("Content-Type", "application/vnd.kafka.binary.v2+json")
	req.Header.Set("Authorization", "Basic "+basicAuth(c.username, c.password))

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("create consumer: %s", body)
	}

	var result struct {
		InstanceID string `json:"instance_id"`
		BaseURI    string `json:"base_uri"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}
	c.instanceID = result.InstanceID
	c.restURL = result.BaseURI
	return nil
}

func (c *RestConsumer) subscribe() error {
	url := fmt.Sprintf("%s/topics/%s", c.restURL, c.topic)
	req, _ := http.NewRequest("POST", url, nil)
	req.Header.Set("Authorization", "Basic "+basicAuth(c.username, c.password))
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("subscribe: %s", body)
	}
	return nil
}

func (c *RestConsumer) Start(ctx context.Context) error {
	url := fmt.Sprintf("%s/records", c.restURL)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
		req.Header.Set("Accept", "application/vnd.kafka.binary.v2+json")
		req.Header.Set("Authorization", "Basic "+basicAuth(c.username, c.password))

		resp, err := c.client.Do(req)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		if resp.StatusCode == http.StatusNoContent {
			resp.Body.Close()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			time.Sleep(time.Second)
			continue
		}

		var records struct {
			Records []struct {
				Value string `json:"value"`
				Key   string `json:"key"`
			} `json:"records"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&records); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()

		for _, rec := range records.Records {
			// Декодируем ключ
			keyBytes, err := base64.StdEncoding.DecodeString(rec.Key)
			if err != nil {
				continue
			}
			keySchemaID, keyData, err := decodeMessage(keyBytes)
			if err != nil || keySchemaID != c.expectedSchemaIDKey {
				continue
			}
			_ = string(keyData) // может пригодиться для логирования

			// Декодируем значение
			valueBytes, err := base64.StdEncoding.DecodeString(rec.Value)
			if err != nil {
				continue
			}
			valSchemaID, protoBytes, err := decodeMessage(valueBytes)
			if err != nil || valSchemaID != c.expectedSchemaIDValue {
				continue
			}

			var pbEvent event.Event
			if err := proto.Unmarshal(protoBytes, &pbEvent); err != nil {
				continue
			}

			stored := &model.StoredEvent{
				ID:            pbEvent.Id,
				SourceSystem:  pbEvent.SourceSystem,
				EventTime:     pbEvent.EventTime.AsTime(),
				PublishedTime: pbEvent.PublishedTime.AsTime(),
				Initiator:     pbEvent.Initiator,
				StateBefore:   pbEvent.StateBefore,
				StateAfter:    pbEvent.StateAfter,
				ChangeTag:     pbEvent.ChangeTag,
			}
			if err := c.repo.Save(ctx, stored); err != nil {
				// логируем ошибку, но продолжаем
				continue
			}
			// TODO: коммит смещения
		}
	}
}

func decodeMessage(data []byte) (int, []byte, error) {
	if len(data) < 5 {
		return 0, nil, fmt.Errorf("too short")
	}
	if data[0] != 0x00 {
		return 0, nil, fmt.Errorf("invalid magic byte")
	}
	schemaID := int(binary.BigEndian.Uint32(data[1:5]))
	return schemaID, data[5:], nil
}

func basicAuth(user, pass string) string {
	return base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
}
