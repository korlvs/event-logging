FROM golang:1.24-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
COPY contracts/event contracts/event
COPY libs/go-outbox libs/go-outbox
COPY services/event-sink services/event-sink

WORKDIR /build/services/event-sink
RUN CGO_ENABLED=0 go build -o /bin/event-sink ./cmd/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /bin/event-sink /bin/event-sink
EXPOSE 8080
ENTRYPOINT ["/bin/event-sink"]