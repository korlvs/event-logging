package api

import (
	"encoding/csv"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/korlvs/event-logging/services/event-sink/internal/model"
	"github.com/labstack/echo/v4"
	"gorm.io/gorm"
)

type AuditServer struct {
	db *gorm.DB
}

func NewAuditServer(db *gorm.DB) *AuditServer {
	return &AuditServer{db: db}
}

// ListAuditEvents обрабатывает GET /audit/events
func (s *AuditServer) ListAuditEvents(ctx echo.Context, params ListAuditEventsParams) error {
	query := s.db.WithContext(ctx.Request().Context()).Model(&model.AuditEvent{})

	if params.Category != nil {
		query = query.Where("category = ?", *params.Category)
	}
	if params.Action != nil {
		query = query.Where("action = ?", *params.Action)
	}
	if params.OperationType != nil {
		query = query.Where("operation_type = ?", *params.OperationType)
	}
	if params.Status != nil {
		query = query.Where("status = ?", *params.Status)
	}
	if params.ActorId != nil {
		query = query.Where("actor_id = ?", *params.ActorId)
	}
	if params.CorrelationId != nil {
		query = query.Where("correlation_id = ?", *params.CorrelationId)
	}
	if params.SourceService != nil {
		query = query.Where("source_service = ?", *params.SourceService)
	}
	if params.Search != nil {
		search := "%" + *params.Search + "%"
		query = query.Where(
			"actor_display_name ILIKE ? OR resource_id ILIKE ? OR details::text ILIKE ?",
			search, search, search,
		)
	}
	if params.From != nil {
		query = query.Where("timestamp >= ?", *params.From)
	}
	if params.To != nil {
		query = query.Where("timestamp <= ?", *params.To)
	}
	if params.EventType != nil {
		query = query.Where("action = ?", *params.EventType)
	}
	if params.IsName != nil {
		// Поиск в JSONB поле resource_details по ключу is_name
		query = query.Where("resource_details->>'is_name' ILIKE ?", "%"+*params.IsName+"%")
	}
	if params.ActorDisplayName != nil {
		query = query.Where("actor_display_name ILIKE ?", "%"+*params.ActorDisplayName+"%")
	}

	// Пагинация
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

	var events []model.AuditEvent
	if err := query.Limit(pageSize).Offset(offset).Order("timestamp DESC").Find(&events).Error; err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	items := make([]AuditEvent, len(events))
	for i, e := range events {
		rd := map[string]interface{}(e.ResourceDetails)
		dt := map[string]interface{}(e.Details)
		items[i] = AuditEvent{
			Id:               e.ID,
			EventId:          e.EventID,
			Timestamp:        e.Timestamp,
			Category:         e.Category,
			Action:           e.Action,
			OperationType:    e.OperationType,
			Status:           e.Status,
			ActorId:          &e.ActorID,
			ActorType:        &e.ActorType,
			ActorDisplayName: &e.ActorDisplayName,
			ClientIp:         &e.ClientIP,
			CorrelationId:    &e.CorrelationID,
			SourceService:    &e.SourceService,
			Environment:      &e.Environment,
			UserAgent:        &e.UserAgent,
			ResourceId:       &e.ResourceID,
			ResourceType:     &e.ResourceType,
			ResourceDetails:  &rd,
			Details:          &dt,
			SchemaVersion:    &e.SchemaVersion,
			CreatedAt:        &e.CreatedAt,
		}
	}

	totalPages := (total + int64(pageSize) - 1) / int64(pageSize)
	return ctx.JSON(http.StatusOK, AuditEventListResponse{
		Items:      items,
		Total:      total,
		Page:       page,
		PageSize:   pageSize,
		TotalPages: int(totalPages),
	})
}

// GetAuditEventById обрабатывает GET /audit/events/{id}
func (s *AuditServer) GetAuditEventById(ctx echo.Context, id string) error {
	var event model.AuditEvent
	if err := s.db.WithContext(ctx.Request().Context()).First(&event, "id = ?", id).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return ctx.NoContent(http.StatusNotFound)
		}
		return ctx.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	rd := map[string]interface{}(event.ResourceDetails)
	dt := map[string]interface{}(event.Details)

	resp := AuditEvent{
		Id:               event.ID,
		EventId:          event.EventID,
		Timestamp:        event.Timestamp,
		Category:         event.Category,
		Action:           event.Action,
		OperationType:    event.OperationType,
		Status:           event.Status,
		ActorId:          &event.ActorID,
		ActorType:        &event.ActorType,
		ActorDisplayName: &event.ActorDisplayName,
		ClientIp:         &event.ClientIP,
		CorrelationId:    &event.CorrelationID,
		SourceService:    &event.SourceService,
		Environment:      &event.Environment,
		UserAgent:        &event.UserAgent,
		ResourceId:       &event.ResourceID,
		ResourceType:     &event.ResourceType,
		ResourceDetails:  &rd,
		Details:          &dt,
		SchemaVersion:    &event.SchemaVersion,
		CreatedAt:        &event.CreatedAt,
	}
	return ctx.JSON(http.StatusOK, resp)
}

// ExportAuditEventsCsv обрабатывает POST /audit/export/csv
func (s *AuditServer) ExportAuditEventsCsv(ctx echo.Context) error {
	var filters ExportFilters
	if err := ctx.Bind(&filters); err != nil {
		return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request body"})
	}

	query := s.db.WithContext(ctx.Request().Context()).Model(&model.AuditEvent{})

	// Применяем все фильтры (аналогично ListAuditEvents)
	if filters.From != nil {
		query = query.Where("timestamp >= ?", *filters.From)
	}
	if filters.To != nil {
		query = query.Where("timestamp <= ?", *filters.To)
	}
	if filters.EventType != nil {
		query = query.Where("action = ?", *filters.EventType)
	}
	if filters.Status != nil {
		query = query.Where("status = ?", *filters.Status)
	}
	if filters.IsName != nil {
		query = query.Where("resource_details->>'is_name' ILIKE ?", "%"+*filters.IsName+"%")
	}
	if filters.ActorDisplayName != nil {
		query = query.Where("actor_display_name ILIKE ?", "%"+*filters.ActorDisplayName+"%")
	}
	if filters.Category != nil {
		query = query.Where("category = ?", *filters.Category)
	}
	if filters.Action != nil {
		query = query.Where("action = ?", *filters.Action)
	}
	if filters.OperationType != nil {
		query = query.Where("operation_type = ?", *filters.OperationType)
	}
	if filters.ActorId != nil {
		query = query.Where("actor_id = ?", *filters.ActorId)
	}
	if filters.CorrelationId != nil {
		query = query.Where("correlation_id = ?", *filters.CorrelationId)
	}
	if filters.SourceService != nil {
		query = query.Where("source_service = ?", *filters.SourceService)
	}
	if filters.Search != nil {
		search := "%" + *filters.Search + "%"
		query = query.Where("actor_display_name ILIKE ? OR resource_id ILIKE ? OR details::text ILIKE ?", search, search, search)
	}

	// Лимит 10000, сортировка по убыванию времени
	var events []model.AuditEvent
	if err := query.Limit(10000).Order("timestamp DESC").Find(&events).Error; err != nil {
		return ctx.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	// Формируем CSV
	ctx.Response().Header().Set(echo.HeaderContentType, "text/csv")
	ctx.Response().Header().Set(echo.HeaderContentDisposition, "attachment; filename=audit_events.csv")
	w := csv.NewWriter(ctx.Response().Writer)
	defer w.Flush()

	// Заголовки
	headers := []string{
		"id", "event_id", "timestamp", "category", "action", "operation_type", "status",
		"actor_id", "actor_type", "actor_display_name", "client_ip", "correlation_id",
		"source_service", "environment", "user_agent", "resource_id", "resource_type",
		"resource_details", "details", "schema_version", "created_at",
	}
	if err := w.Write(headers); err != nil {
		return err
	}

	// Заполнение строк
	for _, e := range events {
		// Преобразуем JSONB поля в строки
		resourceDetailsStr := ""
		if e.ResourceDetails != nil {
			b, _ := json.Marshal(e.ResourceDetails)
			resourceDetailsStr = string(b)
		}
		detailsStr := ""
		if e.Details != nil {
			b, _ := json.Marshal(e.Details)
			detailsStr = string(b)
		}
		row := []string{
			e.ID, e.EventID, e.Timestamp.Format(time.RFC3339), e.Category, e.Action,
			strconv.Itoa(e.OperationType), strconv.Itoa(e.Status),
			e.ActorID, e.ActorType, e.ActorDisplayName,
			e.ClientIP, e.CorrelationID, e.SourceService, e.Environment, e.UserAgent,
			e.ResourceID, e.ResourceType,
			resourceDetailsStr, detailsStr,
			e.SchemaVersion, e.CreatedAt.Format(time.RFC3339),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}
