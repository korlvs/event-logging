package outbox

import (
	"context"
	"fmt"
	"sync"

	eventpb "github.com/korlvs/event-logging/contracts/event"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

var (
	globalInstance *Outbox
	once           sync.Once
)

type Outbox struct {
	db      *gorm.DB
	cfg     Config
	sender  Sender
	worker  *Worker
	encoder Encoder
}

func Init(db *gorm.DB, cfg Config) error {
	var err error
	once.Do(func() {
		globalInstance = &Outbox{db: db, cfg: cfg}
		err = globalInstance.setup()
	})
	return err
}

func (o *Outbox) setup() error {
	// Автоматическое создание таблицы outbox (миграция)
	if err := o.db.AutoMigrate(&OutboxRecord{}); err != nil {
		return err
	}

	// Выбор режима
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

func PublishEvent(ctx context.Context, key string, event *eventpb.Event) error {
	if globalInstance == nil {
		return ErrNotInitialized
	}
	protoBytes, err := proto.Marshal(event)
	if err != nil {
		return err
	}
	record := OutboxRecord{
		EventKey: key,
		Payload:  protoBytes,
	}
	return globalInstance.db.WithContext(ctx).Create(&record).Error
}

func Shutdown() {
	if globalInstance != nil && globalInstance.worker != nil {
		globalInstance.worker.Stop()
	}
	if closer, ok := globalInstance.sender.(interface{ Close() error }); ok {
		closer.Close()
	}
}
