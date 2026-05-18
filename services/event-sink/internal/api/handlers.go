package api

import (
	"net/http"

	"github.com/korlvs/event-logging/services/event-sink/internal/model"
	"github.com/labstack/echo/v4"
	"gorm.io/gorm"
)

type Server struct {
	db *gorm.DB
}

func NewServer(db *gorm.DB) *Server {
	return &Server{db: db}
}

func (s *Server) ListEvents(ctx echo.Context, params ListEventsParams) error {
	query := s.db.WithContext(ctx.Request().Context()).Model(&model.StoredEvent{})

	if params.SourceSystem != nil {
		query = query.Where("source_system = ?", *params.SourceSystem)
	}
	if params.ChangeTag != nil {
		query = query.Where("change_tag = ?", *params.ChangeTag)
	}
	if params.Search != nil {
		search := "%" + *params.Search + "%"
		query = query.Where("state_before ILIKE ? OR state_after ILIKE ?", search, search)
	}
	if params.From != nil {
		query = query.Where("event_time >= ?", *params.From)
	}
	if params.To != nil {
		query = query.Where("event_time <= ?", *params.To)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	limit := 100
	if params.Limit != nil {
		limit = *params.Limit
	}
	offset := 0
	if params.Offset != nil {
		offset = *params.Offset
	}

	var events []model.StoredEvent
	if err := query.Limit(limit).Offset(offset).Order("event_time DESC").Find(&events).Error; err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	items := make([]StoredEvent, len(events))
	for i, e := range events {
		items[i] = StoredEvent{
			Id:            e.ID,
			SourceSystem:  e.SourceSystem,
			EventTime:     e.EventTime,
			PublishedTime: e.PublishedTime,
			Initiator:     e.Initiator,
			StateBefore:   &e.StateBefore,
			StateAfter:    &e.StateAfter,
			ChangeTag:     e.ChangeTag,
			CreatedAt:     &e.CreatedAt,
		}
	}

	return ctx.JSON(http.StatusOK, EventListResponse{Items: items, Total: total})
}

func (s *Server) GetEventById(ctx echo.Context, id string) error {
	var event model.StoredEvent
	if err := s.db.WithContext(ctx.Request().Context()).First(&event, "id = ?", id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return ctx.NoContent(http.StatusNotFound)
		}
		return ctx.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	resp := StoredEvent{
		Id:            event.ID,
		SourceSystem:  event.SourceSystem,
		EventTime:     event.EventTime,
		PublishedTime: event.PublishedTime,
		Initiator:     event.Initiator,
		StateBefore:   &event.StateBefore,
		StateAfter:    &event.StateAfter,
		ChangeTag:     event.ChangeTag,
		CreatedAt:     &event.CreatedAt,
	}
	return ctx.JSON(http.StatusOK, resp)
}
