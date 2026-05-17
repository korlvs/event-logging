package outbox

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Publisher struct {
	client  *http.Client
	restURL string
	topic   string
	auth    string
}

func NewPublisher(cfg Config) *Publisher {
	auth := base64.StdEncoding.EncodeToString([]byte(cfg.KafkaUsername + ":" + cfg.KafkaPassword))
	return &Publisher{
		client:  &http.Client{Timeout: 10 * time.Second},
		restURL: cfg.KafkaRestURL,
		topic:   cfg.KafkaTopic,
		auth:    auth,
	}
}

func (p *Publisher) Send(ctx context.Context, key string, encodedKey, encodedValue []byte) error {
	record := map[string]interface{}{
		"value": base64.StdEncoding.EncodeToString(encodedValue),
	}
	if len(encodedKey) > 0 {
		record["key"] = base64.StdEncoding.EncodeToString(encodedKey)
	} else {
		record["key"] = key
	}
	body, _ := json.Marshal(map[string]interface{}{"records": []interface{}{record}})
	url := fmt.Sprintf("%s/topics/%s", p.restURL, p.topic)
	req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/vnd.kafka.binary.v2+json")
	req.Header.Set("Authorization", "Basic "+p.auth)

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("kafka rest error: %d", resp.StatusCode)
	}
	return nil
}
