package outbox

type Config struct {
	KafkaRestURL  string
	KafkaTopic    string
	KafkaUsername string
	KafkaPassword string
	SchemaIDKey   int // ID схемы ключа (Avro "string")
	SchemaIDValue int // ID схемы значения (Protobuf)
	Workers       int
	BatchSize     int
}
