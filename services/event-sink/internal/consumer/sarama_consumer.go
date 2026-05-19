package consumer

import (
	"context"
	"log"

	"github.com/IBM/sarama"
	event "github.com/korlvs/event-logging/contracts/event/v1"
	"github.com/korlvs/event-logging/services/event-sink/internal/model"
	"github.com/korlvs/event-logging/services/event-sink/internal/repository"
	"google.golang.org/protobuf/proto"
)

type SaramaConsumer struct {
	consumer sarama.ConsumerGroup
	repo     repository.EventRepository
	topic    string
}

func NewSaramaConsumer(brokers []string, groupID, topic string, repo repository.EventRepository) (*SaramaConsumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	cg, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, err
	}
	return &SaramaConsumer{
		consumer: cg,
		repo:     repo,
		topic:    topic,
	}, nil
}

func (c *SaramaConsumer) Start(ctx context.Context) error {
	handler := &saramaHandler{repo: c.repo}
	for {
		if err := c.consumer.Consume(ctx, []string{c.topic}, handler); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

type saramaHandler struct {
	repo repository.EventRepository
}

func (h *saramaHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *saramaHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *saramaHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var pbEvent event.Event
		if err := proto.Unmarshal(msg.Value, &pbEvent); err != nil {
			log.Printf("unmarshal error: %v", err)
			sess.MarkMessage(msg, "")
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
			Tag:           pbEvent.Tag,
			EventType:     pbEvent.EventType,
			Status:        pbEvent.Status,
			Description:   pbEvent.Description,
			TraceID:       pbEvent.TraceId,
		}
		if err := h.repo.Save(sess.Context(), stored); err != nil {
			log.Printf("save error: %v", err)
			continue
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}
