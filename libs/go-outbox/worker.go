package outbox

import (
	"context"
	"log"
	"time"

	"gorm.io/gorm"
)

type Worker struct {
	db      *gorm.DB
	sender  Sender
	encoder Encoder
	cfg     Config
	stopCh  chan struct{}
}

func NewWorker(db *gorm.DB, sender Sender, encoder Encoder, cfg Config) *Worker {
	return &Worker{
		db:      db,
		sender:  sender,
		encoder: encoder,
		cfg:     cfg,
		stopCh:  make(chan struct{}),
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
		encodedKey, encodedValue, err := w.encoder.Encode(rec.EventKey, rec.Payload)
		if err != nil {
			log.Printf("encode failed %s: %v", rec.ID, err)
			continue
		}
		if err := w.sender.Send(ctx, rec.EventKey, encodedKey, encodedValue); err != nil {
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
