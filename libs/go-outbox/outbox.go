package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	eventpb "github.com/korlvs/event-logging/contracts/event/v1"
	"google.golang.org/protobuf/proto"
)

var (
	globalInstance *Outbox
	once           sync.Once
)

type Outbox struct {
	db      *sql.DB
	cfg     Config
	sender  Sender
	encoder Encoder
	worker  *Worker
}

func Init(db *sql.DB, cfg Config) error {
	var err error
	once.Do(func() {
		globalInstance = &Outbox{db: db, cfg: cfg}
		err = globalInstance.setup()
	})
	return err
}

func (o *Outbox) setup() error {
	// Выбор encoder и sender
	switch o.cfg.Mode {
	case "schema-registry":
		o.encoder = NewSchemaRegistryEncoder(o.cfg.SchemaIDKey, o.cfg.SchemaIDValue)
		sender, err := NewRestSender(o.cfg)
		if err != nil {
			return err
		}
		o.sender = sender
	case "binary":
		o.encoder = NewBinaryEncoder()
		sender, err := NewSaramaSender(o.cfg.KafkaBrokers, o.cfg.KafkaTopic)
		if err != nil {
			return err
		}
		o.sender = sender
	default:
		return fmt.Errorf("unknown mode: %s", o.cfg.Mode)
	}

	o.worker = NewWorker(o.db, o.sender, o.encoder, o.cfg)
	go o.worker.Start()
	return nil
}

// MigrateUp применяет встроенные миграции библиотеки по переданному DSN.
func MigrateUp(databaseDSN string) error {
	src, err := iofs.New(MigrationsFS, "migrations")
	if err != nil {
		return err
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, databaseDSN)
	if err != nil {
		return err
	}
	return m.Up()
}

// PublishEvent сохраняет событие вне транзакции.
func PublishEvent(ctx context.Context, key string, event *eventpb.Event) error {
	if globalInstance == nil {
		return ErrNotInitialized
	}
	protoBytes, err := proto.Marshal(event)
	if err != nil {
		return err
	}
	_, err = globalInstance.db.ExecContext(ctx,
		"INSERT INTO outbox (event_key, payload) VALUES ($1, $2)",
		key, protoBytes)
	return err
}

// PublishEventWithTx сохраняет событие в рамках переданной транзакции.
func PublishEventWithTx(ctx context.Context, tx *sql.Tx, key string, event *eventpb.Event) error {
	if globalInstance == nil {
		return ErrNotInitialized
	}
	protoBytes, err := proto.Marshal(event)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		"INSERT INTO outbox (event_key, payload) VALUES ($1, $2)",
		key, protoBytes)
	return err
}

func Shutdown() {
	if globalInstance != nil && globalInstance.worker != nil {
		globalInstance.worker.Stop()
	}
}
