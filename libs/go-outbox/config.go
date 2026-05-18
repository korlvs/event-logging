package outbox

type Config struct {
	// Общие
	Mode       string // "schema-registry" или "binary"
	KafkaTopic string
	Workers    int
	BatchSize  int

	// Для режима schema-registry
	KafkaRestURL  string
	KafkaUsername string
	KafkaPassword string
	SchemaIDKey   int
	SchemaIDValue int

	// Для режима binary
	KafkaBrokers []string
}
