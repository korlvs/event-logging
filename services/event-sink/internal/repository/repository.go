package repository

import (
	"context"

	"github.com/korlvs/event-logging/services/event-sink/internal/model"
	"gorm.io/gorm"
)

type AuditEventRepository interface {
	Save(ctx context.Context, event *model.AuditEvent) error
}

type PostgresEventRepository struct {
	db *gorm.DB
}

func NewPostgresEventRepository(db *gorm.DB) *PostgresEventRepository {
	return &PostgresEventRepository{db: db}
}

func (r *PostgresEventRepository) Save(ctx context.Context, event *model.AuditEvent) error {
	return r.db.WithContext(ctx).Create(event).Error
}
