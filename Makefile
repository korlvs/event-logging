.PHONY: help proto oapi generate clean

# Установка путей
PROTO_DIR := contracts/event
PROTO_OUT := contracts/event/v1
OAPI_INPUT := services/event-sink/api/openapi.yaml
OAPI_OUT := services/event-sink/internal/api/types.gen.go

# Опционально: имя организации (можно переопределить)
ORG_NAME ?= yourorg

help:
	@echo "Доступные команды:"
	@echo "  make proto     - сгенерировать Go код из protobuf"
	@echo "  make oapi      - сгенерировать серверный код из OpenAPI"
	@echo "  make generate  - выполнить обе генерации"
	@echo "  make clean     - удалить сгенерированные файлы"

proto:
	@echo "Генерация protobuf кода..."
	@mkdir -p $(PROTO_OUT)
	protoc --proto_path=$(PROTO_DIR) \
		--go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/event.proto
	@echo "✅ Protobuf готов"

oapi:
	@echo "Генерация OpenAPI серверного кода..."
	@cd services/event-sink && oapi-codegen -package api -generate types,server,spec -o internal/api/types.gen.go api/openapi.yaml
	@echo "✅ OpenAPI код готов"

generate: proto oapi
	@echo "✅ Генерация завершена"

clean:
	@echo "Очистка сгенерированных файлов..."
	@rm -f $(PROTO_OUT)/event/v1/*.pb.go
	@rm -f $(OAPI_OUT)
	@echo "✅ Очистка завершена"