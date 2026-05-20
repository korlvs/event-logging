package outbox

import (
	"context"
	"database/sql"
	"log"
	"time"
)

type Worker struct {
	db      *sql.DB
	sender  Sender
	encoder Encoder
	cfg     Config
	stopCh  chan struct{}
}

func NewWorker(db *sql.DB, sender Sender, encoder Encoder, cfg Config) *Worker {
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
	rows, err := w.db.QueryContext(ctx,
		`SELECT id, event_key, payload FROM outbox
         WHERE published_at IS NULL
         ORDER BY created_at ASC
         LIMIT $1`,
		w.cfg.BatchSize)
	if err != nil {
		log.Printf("fetch outbox: %v", err)
		return
	}
	defer rows.Close()

	type record struct {
		id       string
		eventKey string
		payload  []byte
	}
	var records []record
	for rows.Next() {
		var r record
		if err := rows.Scan(&r.id, &r.eventKey, &r.payload); err != nil {
			log.Printf("scan: %v", err)
			continue
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		log.Printf("rows error: %v", err)
		return
	}

	for _, rec := range records {
		encodedKey, encodedValue, err := w.encoder.Encode(rec.eventKey, rec.payload)
		if err != nil {
			log.Printf("encode failed %s: %v", rec.id, err)
			continue
		}
		if err := w.sender.Send(ctx, rec.eventKey, encodedKey, encodedValue); err != nil {
			log.Printf("send failed %s: %v", rec.id, err)
			continue
		}
		if _, err := w.db.ExecContext(ctx,
			"UPDATE outbox SET published_at = NOW() WHERE id = $1", rec.id); err != nil {
			log.Printf("mark published failed %s: %v", rec.id, err)
		}
	}
}

func (w *Worker) Stop() {
	close(w.stopCh)
}
