package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	eventpb "github.com/korlvs/event-logging/contracts/event"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

var (
	globalInstance *Outbox
	once           sync.Once
)

type Outbox struct {
	db            *gorm.DB
	sqlDB         *sql.DB
	cfg           Config
	pub           *Publisher
	worker        *Worker
	schemaIDKey   int
	schemaIDValue int
}

func Init(db *gorm.DB, cfg Config) error {
	var err error
	once.Do(func() {
		sqlDB, err := db.DB()
		if err != nil {
			return
		}
		globalInstance = &Outbox{
			db:            db,
			sqlDB:         sqlDB,
			cfg:           cfg,
			schemaIDKey:   cfg.SchemaIDKey,
			schemaIDValue: cfg.SchemaIDValue,
		}
		err = globalInstance.setup()
	})
	return err
}

func (o *Outbox) setup() error {
	// Применяем миграции
	driver, err := postgres.WithInstance(o.sqlDB, &postgres.Config{})
	if err != nil {
		return fmt.Errorf("failed to create postgres driver: %w", err)
	}
	src, err := iofs.New(MigrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("failed to create source driver: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "postgres", driver)
	if err != nil {
		return fmt.Errorf("failed to create migrate instance: %w", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to apply migrations: %w", err)
	}

	// Автомиграция через GORM (на всякий случай)
	if err := o.db.AutoMigrate(&OutboxRecord{}); err != nil {
		return err
	}

	o.pub = NewPublisher(o.cfg)
	o.worker = NewWorker(o.db, o.pub, o.cfg, o.schemaIDKey, o.schemaIDValue)
	go o.worker.Start()
	return nil
}

// PublishEvent сохраняет событие в outbox (принимает protobuf-сообщение)
func PublishEvent(ctx context.Context, key string, event *eventpb.Event) error {
	if globalInstance == nil {
		return ErrNotInitialized
	}
	// Сериализуем событие в protobuf
	valueBytes, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	record := OutboxRecord{
		EventKey: key,
		Payload:  valueBytes,
	}
	return globalInstance.db.WithContext(ctx).Create(&record).Error
}

func Shutdown() {
	if globalInstance != nil && globalInstance.worker != nil {
		globalInstance.worker.Stop()
	}
}
