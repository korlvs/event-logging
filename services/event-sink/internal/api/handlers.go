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

// Health возвращает статус сервиса
func (s *Server) Health(ctx echo.Context) error {
	return ctx.JSON(http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ListEvents(ctx echo.Context, params ListEventsParams) error {
	query := s.db.WithContext(ctx.Request().Context()).Model(&model.StoredEvent{})

	if params.SourceSystem != nil {
		query = query.Where("source_system = ?", *params.SourceSystem)
	}
	if params.EventType != nil {
		query = query.Where("event_type = ?", *params.EventType)
	}
	if params.Status != nil {
		query = query.Where("status = ?", *params.Status)
	}
	if params.Tag != nil {
		query = query.Where("tag = ?", *params.Tag)
	}
	if params.Search != nil {
		search := "%" + *params.Search + "%"
		query = query.Where("initiator ILIKE ? OR description ILIKE ?", search, search)
	}
	if params.From != nil {
		query = query.Where("event_time >= ?", *params.From)
	}
	if params.To != nil {
		query = query.Where("event_time <= ?", *params.To)
	}

	// пагинация
	page := 1
	if params.Page != nil {
		page = *params.Page
	}
	pageSize := 20
	if params.PageSize != nil {
		pageSize = *params.PageSize
	}
	offset := (page - 1) * pageSize

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	var events []model.StoredEvent
	if err := query.Limit(pageSize).Offset(offset).Order("event_time DESC").Find(&events).Error; err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	// преобразование в API-модель
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
			Tag:           e.Tag,
			EventType:     e.EventType,
			Status:        &e.Status,
			Description:   &e.Description,
			TraceId:       &e.TraceID,
			CreatedAt:     &e.CreatedAt,
		}
	}

	totalPages := (total + int64(pageSize) - 1) / int64(pageSize)
	return ctx.JSON(http.StatusOK, EventListResponse{
		Items:      items,
		Total:      total,
		Page:       page,
		PageSize:   pageSize,
		TotalPages: int(totalPages),
	})
}

// GetEventById – существующий метод
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
		Tag:           event.Tag,
		EventType:     event.EventType,
		Status:        &event.Status,
		Description:   &event.Description,
		TraceId:       &event.TraceID,
		CreatedAt:     &event.CreatedAt,
	}
	return ctx.JSON(http.StatusOK, resp)
}
