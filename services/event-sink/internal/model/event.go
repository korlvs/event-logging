package model

import (
	"database/sql/driver"
	"encoding/json"
	"time"
)

type AuditEvent struct {
	ID               string    `gorm:"primaryKey;type:uuid;default:gen_random_uuid()"`
	EventID          string    `gorm:"column:event_id;not null;uniqueIndex"`
	Timestamp        time.Time `gorm:"not null;index"`
	Category         string    `gorm:"not null;index"`
	Action           string    `gorm:"not null;index"`
	OperationType    int       `gorm:"column:operation_type;not null;index"`
	Status           int       `gorm:"not null;index"`
	ActorID          string    `gorm:"column:actor_id;index"`
	ActorType        string    `gorm:"column:actor_type"`
	ActorDisplayName string    `gorm:"column:actor_display_name"`
	ClientIP         string    `gorm:"column:client_ip"`
	CorrelationID    string    `gorm:"column:correlation_id;index"`
	SourceService    string    `gorm:"column:source_service;index"`
	Environment      string    `gorm:"column:environment"`
	UserAgent        string    `gorm:"column:user_agent"`
	ResourceID       string    `gorm:"column:resource_id;index"`
	ResourceType     string    `gorm:"column:resource_type;index"`
	ResourceDetails  JSONB     `gorm:"type:jsonb"`
	Details          JSONB     `gorm:"type:jsonb"`
	SchemaVersion    string    `gorm:"column:schema_version"`
	CreatedAt        time.Time `gorm:"autoCreateTime"`
}

func (AuditEvent) TableName() string {
	return "audit_events"
}

type JSONB map[string]interface{}

func (j JSONB) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return json.Marshal(j)
}

func (j *JSONB) Scan(value interface{}) error {
	if value == nil {
		*j = nil
		return nil
	}
	bytes, ok := value.([]byte)
	if !ok {
		return nil
	}
	return json.Unmarshal(bytes, j)
}
