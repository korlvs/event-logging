package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/korlvs/event-logging/services/event-sink/internal/api"
	"github.com/korlvs/event-logging/services/event-sink/internal/consumer"
	"github.com/korlvs/event-logging/services/event-sink/internal/repository"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL not set")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	// Миграции
	m, err := migrate.New("file://internal/migrations", dsn)
	if err != nil {
		log.Fatal(err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatal(err)
	}

	repo := repository.NewPostgresEventRepository(db)

	mode := os.Getenv("MODE")
	if mode == "" {
		mode = "binary"
	}

	ctx, cancel := context.WithCancel(context.Background())
	switch mode {
	case "schema-registry":
		kafkaRestURL := os.Getenv("KAFKA_REST_URL")
		if kafkaRestURL == "" {
			log.Fatal("KAFKA_REST_URL not set")
		}
		kafkaTopic := os.Getenv("KAFKA_TOPIC")
		if kafkaTopic == "" {
			log.Fatal("KAFKA_TOPIC not set")
		}
		kafkaGroup := os.Getenv("KAFKA_GROUP")
		if kafkaGroup == "" {
			kafkaGroup = "event-sink-group"
		}
		kafkaUser := os.Getenv("KAFKA_USERNAME")
		kafkaPass := os.Getenv("KAFKA_PASSWORD")

		schemaIDKey, _ := strconv.Atoi(os.Getenv("SCHEMA_ID_KEY"))
		schemaIDValue, _ := strconv.Atoi(os.Getenv("SCHEMA_ID_VALUE"))
		if schemaIDKey == 0 || schemaIDValue == 0 {
			log.Fatal("SCHEMA_ID_KEY and SCHEMA_ID_VALUE must be set in schema-registry mode")
		}

		cons, err := consumer.NewRestConsumer(
			kafkaRestURL, kafkaTopic, kafkaGroup, kafkaUser, kafkaPass,
			repo, schemaIDKey, schemaIDValue,
		)
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			if err := cons.Start(ctx); err != nil {
				log.Printf("rest consumer error: %v", err)
			}
		}()

	case "binary":
		kafkaBrokersEnv := os.Getenv("KAFKA_BROKERS")
		if kafkaBrokersEnv == "" {
			log.Fatal("KAFKA_BROKERS not set")
		}
		kafkaBrokers := []string{kafkaBrokersEnv}
		kafkaTopic := os.Getenv("KAFKA_TOPIC")
		if kafkaTopic == "" {
			log.Fatal("KAFKA_TOPIC not set")
		}
		kafkaGroup := os.Getenv("KAFKA_GROUP")
		if kafkaGroup == "" {
			kafkaGroup = "event-sink-group"
		}

		cons, err := consumer.NewSaramaConsumer(kafkaBrokers, kafkaGroup, kafkaTopic, repo)
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			if err := cons.Start(ctx); err != nil {
				log.Printf("sarama consumer error: %v", err)
			}
		}()

	default:
		log.Fatalf("unknown MODE: %s. Allowed: binary, schema-registry", mode)
	}

	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	srv := api.NewServer(db)
	api.RegisterHandlers(e, srv)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		cancel()
		if err := e.Close(); err != nil {
			log.Printf("error closing echo: %v", err)
		}
	}()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("starting server on :%s (mode=%s)", port, mode)
	log.Fatal(e.Start(":" + port))
}
