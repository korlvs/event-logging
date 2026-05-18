package model

import "time"

type StoredEvent struct {
	ID            string    `gorm:"primaryKey;type:uuid;default:gen_random_uuid()"`
	SourceSystem  string    `gorm:"index;not null"`
	EventTime     time.Time `gorm:"index;not null"`
	PublishedTime time.Time `gorm:"index;not null"`
	Initiator     string    `gorm:"not null"`
	StateBefore   string    `gorm:"type:text"`
	StateAfter    string    `gorm:"type:text"`
	ChangeTag     string    `gorm:"index;not null"`
	CreatedAt     time.Time `gorm:"autoCreateTime"`
}

func (StoredEvent) TableName() string {
	return "events"
}
