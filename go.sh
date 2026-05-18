cat > services/event-sink/internal/api/handlers.go << 'EOF'
package api

import (
    "encoding/csv"
    "encoding/json"
    "net/http"
    "time"
    "github.com/labstack/echo/v4"
    "gorm.io/gorm"
    "github.com/yourorg/event-sink/internal/model"
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
        query = query.Where("state_before::text ILIKE ? OR state_after::text ILIKE ?", search, search)
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
        var stateBeforeMap, stateAfterMap map[string]interface{}
        if len(e.StateBefore) > 0 {
            json.Unmarshal(e.StateBefore, &stateBeforeMap)
        }
        if len(e.StateAfter) > 0 {
            json.Unmarshal(e.StateAfter, &stateAfterMap)
        }
        items[i] = StoredEvent{
            Id:            e.ID,
            SourceSystem:  e.SourceSystem,
            EventTime:     e.EventTime,
            PublishedTime: e.PublishedTime,
            Initiator:     e.Initiator,
            StateBefore:   &stateBeforeMap,
            StateAfter:    &stateAfterMap,
            ChangeTag:     e.ChangeTag,
            CreatedAt:     &e.CreatedAt,
        }
    }
    return ctx.JSON(http.StatusOK, ListEventsResponse{Items: items, Total: &total})
}

func (s *Server) GetEventById(ctx echo.Context, id string) error {
    var event model.StoredEvent
    if err := s.db.WithContext(ctx.Request().Context()).First(&event, "id = ?", id).Error; err != nil {
        if err == gorm.ErrRecordNotFound {
            return ctx.NoContent(http.StatusNotFound)
        }
        return ctx.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
    }
    var stateBeforeMap, stateAfterMap map[string]interface{}
    if len(event.StateBefore) > 0 {
        json.Unmarshal(event.StateBefore, &stateBeforeMap)
    }
    if len(event.StateAfter) > 0 {
        json.Unmarshal(event.StateAfter, &stateAfterMap)
    }
    resp := StoredEvent{
        Id:            event.ID,
        SourceSystem:  event.SourceSystem,
        EventTime:     event.EventTime,
        PublishedTime: event.PublishedTime,
        Initiator:     event.Initiator,
        StateBefore:   &stateBeforeMap,
        StateAfter:    &stateAfterMap,
        ChangeTag:     event.ChangeTag,
        CreatedAt:     &event.CreatedAt,
    }
    return ctx.JSON(http.StatusOK, resp)
}

func (s *Server) ExportCSV(ctx echo.Context) error {
    var req struct {
        Filters struct {
            SourceSystem *string    `json:"source_system"`
            ChangeTag    *string    `json:"change_tag"`
            Search       *string    `json:"search"`
            From         *string    `json:"from"`
            To           *string    `json:"to"`
        } `json:"filters"`
    }
    if err := ctx.Bind(&req); err != nil {
        return ctx.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request"})
    }
    query := s.db.WithContext(ctx.Request().Context()).Model(&model.StoredEvent{})
    if req.Filters.SourceSystem != nil {
        query = query.Where("source_system = ?", *req.Filters.SourceSystem)
    }
    if req.Filters.ChangeTag != nil {
        query = query.Where("change_tag = ?", *req.Filters.ChangeTag)
    }
    if req.Filters.Search != nil {
        search := "%" + *req.Filters.Search + "%"
        query = query.Where("state_before::text ILIKE ? OR state_after::text ILIKE ?", search, search)
    }
    if req.Filters.From != nil {
        query = query.Where("event_time >= ?", *req.Filters.From)
    }
    if req.Filters.To != nil {
        query = query.Where("event_time <= ?", *req.Filters.To)
    }
    var events []model.StoredEvent
    if err := query.Order("event_time DESC").Find(&events).Error; err != nil {
        return ctx.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
    }
    ctx.Response().Header().Set(echo.HeaderContentType, "text/csv")
    ctx.Response().Header().Set(echo.HeaderContentDisposition, "attachment; filename=events.csv")
    w := csv.NewWriter(ctx.Response().Writer)
    headers := []string{"id","source_system","event_time","published_time","initiator","state_before","state_after","change_tag","created_at"}
    if err := w.Write(headers); err != nil {
        return err
    }
    for _, e := range events {
        var stateBefore, stateAfter interface{}
        if len(e.StateBefore) > 0 {
            var m map[string]interface{}
            json.Unmarshal(e.StateBefore, &m)
            stateBefore = m
        }
        if len(e.StateAfter) > 0 {
            var m map[string]interface{}
            json.Unmarshal(e.StateAfter, &m)
            stateAfter = m
        }
        beforeStr := ""
        afterStr := ""
        if stateBefore != nil {
            b, _ := json.Marshal(stateBefore)
            beforeStr = string(b)
        }
        if stateAfter != nil {
            b, _ := json.Marshal(stateAfter)
            afterStr = string(b)
        }
        row := []string{
            e.ID, e.SourceSystem, e.EventTime.Format(time.RFC3339), e.PublishedTime.Format(time.RFC3339),
            e.Initiator, beforeStr, afterStr, e.ChangeTag, e.CreatedAt.Format(time.RFC3339),
        }
        if err := w.Write(row); err != nil {
            return err
        }
    }
    w.Flush()
    return nil
}
EOF