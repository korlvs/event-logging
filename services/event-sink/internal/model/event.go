package model

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type StoredEvent struct {
	ID            string    `gorm:"primaryKey;column:id"`
	SourceSystem  string    `gorm:"index;not null"`
	EventTime     time.Time `gorm:"index;not null"`
	PublishedTime time.Time `gorm:"index;not null"`
	Initiator     string    `gorm:"not null"`
	StateBefore   string    `gorm:"type:text"`
	StateAfter    string    `gorm:"type:text"`
	Tag           string    `gorm:"index;not null"`
	EventType     string    `gorm:"index;not null"`
	Status        string    `gorm:"index"`
	Description   string    `gorm:"type:text"`
	TraceID       string    `gorm:"index"` // новое поле
	CreatedAt     time.Time `gorm:"autoCreateTime"`
}

func (StoredEvent) TableName() string {
	return "events"
}

func (e *StoredEvent) BeforeCreate(tx *gorm.DB) error {
	if e.ID == "" {
		e.ID = uuid.New().String()
	}
	return nil
}
