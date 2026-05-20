package outbox

type Config struct {
	Mode       string // "binary" или "schema-registry"
	KafkaTopic string
	Workers    int
	BatchSize  int

	// для schema-registry
	KafkaRestURL  string
	KafkaUsername string
	KafkaPassword string
	SchemaIDKey   int
	SchemaIDValue int

	// для binary
	KafkaBrokers []string
}
