package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	echoSwagger "github.com/swaggo/echo-swagger"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/korlvs/event-logging/services/event-sink/internal/api"
	"github.com/korlvs/event-logging/services/event-sink/internal/consumer"
	"github.com/korlvs/event-logging/services/event-sink/internal/migrations"
	"github.com/korlvs/event-logging/services/event-sink/internal/repository"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://eventuser:eventpass@localhost:5432/eventsink?sslmode=disable"
	}

	// Применяем миграции
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		log.Fatal("failed to create migrations source:", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, dsn)
	if err != nil {
		log.Fatal("failed to create migrate instance:", err)
	}
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatal("migration failed:", err)
	}

	// GORM
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("failed to connect to database:", err)
	}

	repo := repository.NewPostgresEventRepository(db)

	// Kafka consumer
	kafkaBrokers := []string{os.Getenv("KAFKA_BROKERS")}
	if len(kafkaBrokers) == 0 || kafkaBrokers[0] == "" {
		kafkaBrokers = []string{"localhost:9092"}
	}
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		kafkaTopic = "audit-events"
	}
	kafkaGroup := os.Getenv("KAFKA_GROUP")
	if kafkaGroup == "" {
		kafkaGroup = "event-sink-group"
	}

	cons, err := consumer.NewSaramaConsumer(kafkaBrokers, kafkaGroup, kafkaTopic, repo)
	if err != nil {
		log.Fatal("failed to create Kafka consumer:", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := cons.Start(ctx); err != nil {
			log.Printf("Kafka consumer error: %v", err)
		}
	}()

	// HTTP сервер
	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Use(middleware.CORS())

	// API хендлеры
	auditServer := api.NewAuditServer(db)
	api.RegisterHandlers(e, auditServer)

	// Health check
	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})

	// Swagger UI
	e.GET("/swagger/*", echoSwagger.WrapHandler)

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		cancel()
		if err := e.Close(); err != nil {
			log.Printf("error closing echo server: %v", err)
		}
	}()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("starting server on :%s", port)
	if err := e.Start(":" + port); err != nil {
		log.Fatal(err)
	}
}
