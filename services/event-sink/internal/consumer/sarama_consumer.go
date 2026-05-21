package consumer

import (
	"context"
	"log"

	"github.com/IBM/sarama"
	eventpb "github.com/korlvs/event-logging/contracts/event/v1"
	"github.com/korlvs/event-logging/services/event-sink/internal/model"
	"github.com/korlvs/event-logging/services/event-sink/internal/repository"
	"google.golang.org/protobuf/proto"
)

type SaramaConsumer struct {
	consumer sarama.ConsumerGroup
	repo     repository.AuditEventRepository
	topic    string
}

func NewSaramaConsumer(brokers []string, groupID, topic string, repo repository.AuditEventRepository) (*SaramaConsumer, error) {
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
	repo repository.AuditEventRepository
}

func (h *saramaHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *saramaHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *saramaHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var pbEvent eventpb.Event
		if err := proto.Unmarshal(msg.Value, &pbEvent); err != nil {
			log.Printf("unmarshal error: %v", err)
			sess.MarkMessage(msg, "")
			continue
		}
		stored := convertToModel(&pbEvent)
		if err := h.repo.Save(sess.Context(), stored); err != nil {
			log.Printf("save error: %v", err)
			continue
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}

func convertToModel(pb *eventpb.Event) *model.AuditEvent {
	ev := &model.AuditEvent{
		EventID:       pb.EventId,
		Timestamp:     pb.Timestamp.AsTime(),
		Category:      pb.Category,
		Action:        pb.Action,
		OperationType: int(pb.OperationType),
		Status:        int(pb.Status),
		SchemaVersion: pb.SchemaVersion,
	}
	if pb.Actor != nil {
		ev.ActorID = pb.Actor.Id
		ev.ActorType = pb.Actor.Type
		ev.ActorDisplayName = pb.Actor.DisplayName
	}
	if pb.Context != nil {
		ev.ClientIP = pb.Context.ClientIp
		ev.CorrelationID = pb.Context.CorrelationId
		ev.SourceService = pb.Context.SourceService
		ev.Environment = pb.Context.Environment
		ev.UserAgent = pb.Context.UserAgent
	}
	if pb.Resource != nil {
		ev.ResourceID = pb.Resource.Id
		ev.ResourceType = pb.Resource.Type
	}
	if pb.ResourceDetails != nil {
		ev.ResourceDetails = pb.ResourceDetails.AsMap()
	}
	if pb.Details != nil {
		ev.Details = pb.Details.AsMap()
	}
	return ev
}
