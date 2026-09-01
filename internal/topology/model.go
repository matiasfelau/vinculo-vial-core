// Package topology define, como datos, la topología declarativa de
// RabbitMQ (exchanges, colas y bindings) que este job aplica y verifica.
package topology

// ExchangeType enumera los tipos de exchange soportados por la topología.
type ExchangeType string

const (
	ExchangeTopic  ExchangeType = "topic"
	ExchangeFanout ExchangeType = "fanout"
)

// QueueKind clasifica el rol de una cola dentro del ciclo de reintento.
type QueueKind string

const (
	QueueKindWork     QueueKind = "work"
	QueueKindRetry    QueueKind = "retry"
	QueueKindDLQ      QueueKind = "dlq"
	QueueKindUnrouted QueueKind = "unrouted"
)

// Exchange describe un exchange a declarar.
type Exchange struct {
	Name      string
	Type      ExchangeType
	Durable   bool
	Arguments map[string]any
}

// Queue describe una cola a declarar.
type Queue struct {
	Name      string
	Kind      QueueKind
	Durable   bool
	Arguments map[string]any
}

// Binding describe el enlace entre una cola y un exchange.
type Binding struct {
	Queue      string
	Exchange   string
	RoutingKey string
}

// Topology es la definición declarativa completa: exchanges, colas y
// bindings como datos, recorridos por un aplicador genérico (ver
// internal/amqp) en lugar de una secuencia de llamadas escritas a mano.
// Agregar la topología de un caso de uso futuro es sumar entradas acá, no
// escribir código nuevo de declaración.
type Topology struct {
	Exchanges []Exchange
	Queues    []Queue
	Bindings  []Binding
}
