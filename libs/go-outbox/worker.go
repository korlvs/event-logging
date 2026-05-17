package outbox

import (
	"context"
	"log"
	"time"

	"gorm.io/gorm"
)

type Worker struct {
	db            *gorm.DB
	pub           *Publisher
	cfg           Config
	schemaIDKey   int
	schemaIDValue int
	stopCh        chan struct{}
}

func NewWorker(db *gorm.DB, pub *Publisher, cfg Config, schemaIDKey, schemaIDValue int) *Worker {
	return &Worker{
		db:            db,
		pub:           pub,
		cfg:           cfg,
		schemaIDKey:   schemaIDKey,
		schemaIDValue: schemaIDValue,
		stopCh:        make(chan struct{}),
	}
}

func (w *Worker) Start() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			w.processBatch()
		case <-w.stopCh:
			return
		}
	}
}

func (w *Worker) processBatch() {
	ctx := context.Background()
	var records []OutboxRecord
	if err := w.db.WithContext(ctx).
		Where("published_at IS NULL").
		Limit(w.cfg.BatchSize).
		Order("created_at ASC").
		Find(&records).Error; err != nil {
		log.Printf("fetch outbox: %v", err)
		return
	}
	for _, rec := range records {
		encodedKey := EncodeMessage(w.schemaIDKey, []byte(rec.EventKey))
		encodedValue := EncodeMessage(w.schemaIDValue, rec.Payload)
		if err := w.pub.Send(ctx, rec.EventKey, encodedKey, encodedValue); err != nil {
			log.Printf("send failed %s: %v", rec.ID, err)
			continue
		}
		now := time.Now()
		if err := w.db.WithContext(ctx).Model(&OutboxRecord{}).
			Where("id = ?", rec.ID).Update("published_at", now).Error; err != nil {
			log.Printf("mark published failed %s: %v", rec.ID, err)
		}
	}
}

func (w *Worker) Stop() {
	close(w.stopCh)
}
