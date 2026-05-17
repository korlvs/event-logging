package outbox

import (
	"time"
)

type OutboxRecord struct {
	ID          string     `gorm:"primaryKey;type:uuid;default:gen_random_uuid()"`
	EventKey    string     `gorm:"index;not null"`
	Payload     []byte     `gorm:"type:bytea;not null"` // сериализованный protobuf (без ID)
	CreatedAt   time.Time  `gorm:"autoCreateTime"`
	PublishedAt *time.Time `gorm:"index"`
}

func (OutboxRecord) TableName() string {
	return "outbox"
}
