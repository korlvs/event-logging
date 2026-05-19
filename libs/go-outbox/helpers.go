package outbox

import (
	"context"

	"github.com/google/uuid"
	eventpb "github.com/korlvs/event-logging/contracts/event/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// NewEvent создаёт событие с заполненными обязательными полями.
func NewEvent(sourceSystem, initiator, eventType, tag, stateBefore, stateAfter string) *eventpb.Event {
	now := timestamppb.Now()
	return &eventpb.Event{
		Id:            uuid.New().String(),
		SourceSystem:  sourceSystem,
		EventTime:     now,
		PublishedTime: now,
		Initiator:     initiator,
		StateBefore:   stateBefore,
		StateAfter:    stateAfter,
		Tag:           tag,
		EventType:     eventType,
		TraceId:       uuid.New().String(),
	}
}

// SetStatus устанавливает статус события.
func SetStatus(event *eventpb.Event, status string) {
	event.Status = status
}

// SetDescription устанавливает описание события.
func SetDescription(event *eventpb.Event, description string) {
	event.Description = description
}

// PublishSimple публикует событие с минимальным набором полей (без статуса и описания).
func PublishSimple(ctx context.Context, sourceSystem, initiator, eventType, tag, stateBefore, stateAfter string) error {
	event := NewEvent(sourceSystem, initiator, eventType, tag, stateBefore, stateAfter)
	return PublishEvent(ctx, event.Id, event)
}
