package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	eventpb "github.com/korlvs/event-logging/contracts/event/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	// Если схема не задана, используем "public"
	if cfg.Schema == "" {
		cfg.Schema = "public"
	}
	var err error
	once.Do(func() {
		globalInstance = &Outbox{db: db, cfg: cfg}
		err = globalInstance.setup()
	})
	return err
}

func (o *Outbox) setup() error {
	if err := RunMigrations(o.db, o.cfg.Schema); err != nil {
		return fmt.Errorf("migrations failed: %w", err)
	}

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

// PublishEvent публикует событие без внешней транзакции.
func PublishEvent(ctx context.Context, key string, event *eventpb.Event) error {
	if globalInstance == nil {
		return ErrNotInitialized
	}
	enrichEvent(ctx, event)
	protoBytes, err := proto.Marshal(event)
	if err != nil {
		return err
	}
	_, err = globalInstance.db.ExecContext(ctx,
		"INSERT INTO outbox (event_key, payload) VALUES ($1, $2)",
		key, protoBytes)
	return err
}

// PublishEventWithTx публикует событие внутри переданной транзакции.
func PublishEventWithTx(ctx context.Context, tx *sql.Tx, key string, event *eventpb.Event) error {
	if globalInstance == nil {
		return ErrNotInitialized
	}
	enrichEvent(ctx, event)
	protoBytes, err := proto.Marshal(event)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		"INSERT INTO outbox (event_key, payload) VALUES ($1, $2)",
		key, protoBytes)
	return err
}

// enrichEvent заполняет недостающие поля события из контекста и конфигурации.
func enrichEvent(ctx context.Context, event *eventpb.Event) {
	// Устанавливаем версию схемы, если не задана
	if event.SchemaVersion == "" {
		event.SchemaVersion = "1.0"
	}
	// Устанавливаем временную метку, если отсутствует
	if event.Timestamp == nil {
		event.Timestamp = timestamppb.Now()
	}

	// Обогащаем Context (поле типа RequestContext)
	if event.Context == nil {
		event.Context = &eventpb.RequestContext{}
	}
	rc := event.Context

	// Извлекаем метаданные из контекста запроса (если есть)
	meta := RequestMetadataFromContext(ctx)
	if meta != nil {
		if rc.ClientIp == "" {
			rc.ClientIp = meta.ClientIP
		}
		if rc.CorrelationId == "" {
			rc.CorrelationId = meta.CorrelationID
		}
		if rc.UserAgent == "" {
			rc.UserAgent = meta.UserAgent
		}
	}

	// Заполняем source_service и environment из конфигурации библиотеки
	if rc.SourceService == "" && globalInstance.cfg.ServiceName != "" {
		rc.SourceService = globalInstance.cfg.ServiceName
	}
	if rc.Environment == "" && globalInstance.cfg.Environment != "" {
		rc.Environment = globalInstance.cfg.Environment
	}

	// Обогащаем Actor (инициатора)
	if event.Actor == nil {
		event.Actor = &eventpb.Actor{}
	}
	if meta != nil {
		if event.Actor.Id == "" {
			event.Actor.Id = meta.UserID
		}
		if event.Actor.DisplayName == "" {
			event.Actor.DisplayName = meta.UserEmail
		}
	}
	if event.Actor.Type == "" {
		if event.Actor.Id != "" {
			event.Actor.Type = "user"
		} else {
			event.Actor.Type = "anonymous"
		}
	}

	// Добавляем в поле Details информацию о версии сервиса и окружении
	if event.Details == nil {
		event.Details = &structpb.Struct{Fields: map[string]*structpb.Value{}}
	}
	if globalInstance.cfg.ServiceVersion != "" {
		event.Details.Fields["service_version"] = structpb.NewStringValue(globalInstance.cfg.ServiceVersion)
	}
	// Дублируем environment в details для удобства, если не задано
	if _, ok := event.Details.Fields["environment"]; !ok && globalInstance.cfg.Environment != "" {
		event.Details.Fields["environment"] = structpb.NewStringValue(globalInstance.cfg.Environment)
	}
}

func Shutdown() {
	if globalInstance != nil && globalInstance.worker != nil {
		globalInstance.worker.Stop()
	}
}
