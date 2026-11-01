package topology

import (
	"strings"

	"github.com/vinculo-vial/core-service/pkg/config"
)

const (
	// DefaultExchange es el exchange default de AMQP (nombre vacío): rutea
	// directo a la cola cuyo nombre coincide con la routing key.
	DefaultExchange = ""

	workSuffix  = ".q"
	retrySuffix = ".retry"
	dlqSuffix   = ".dlq"

	argDeadLetterExchange   = "x-dead-letter-exchange"
	argDeadLetterRoutingKey = "x-dead-letter-routing-key"
	argMessageTTL           = "x-message-ttl"
)

// flowQueues agrupa la cola de trabajo W, su W.retry y su W.dlq, junto con
// los bindings que las conectan a los exchanges principal, de reintento y
// de dead-letter.
type flowQueues struct {
	queues   []Queue
	bindings []Binding
}

// flow construye el ciclo de trabajo/retry/dlq de un consumidor según la
// convención <consumidor>.<evento>: base ya incluye el consumidor y el
// evento (por ejemplo "notification.alert-created"), y esta función agrega
// los sufijos .q, .retry y .dlq.
//
// El retry y la DLQ se rutean por el nombre de la cola de trabajo W, nunca
// por la routing key del evento (eventRoutingKey solo liga W al exchange
// principal). Así un fallo reentrega el mensaje únicamente a W y no a los
// demás consumidores del mismo evento:
//
//	W       dead-letter a vvi.retry.exchange con routing key W
//	W.retry bindeada a vvi.retry.exchange por W; vencido el TTL, dead-letter
//	        al exchange default con routing key W
//	W.dlq   bindeada a vvi.dlq.exchange por W
func flow(base, eventRoutingKey string, cfg config.Config) flowQueues {
	workName := base + workSuffix
	retryName := base + retrySuffix
	dlqName := base + dlqSuffix

	return flowQueues{
		queues: []Queue{
			{
				Name:    workName,
				Kind:    QueueKindWork,
				Durable: true,
				Arguments: map[string]any{
					argDeadLetterExchange:   cfg.RetryExchange,
					argDeadLetterRoutingKey: workName,
				},
			},
			{
				Name:    retryName,
				Kind:    QueueKindRetry,
				Durable: true,
				Arguments: map[string]any{
					argMessageTTL:           cfg.RetryTTLMs,
					argDeadLetterExchange:   DefaultExchange,
					argDeadLetterRoutingKey: workName,
				},
			},
			{Name: dlqName, Kind: QueueKindDLQ, Durable: true},
		},
		bindings: []Binding{
			{Queue: workName, Exchange: cfg.MainExchange, RoutingKey: eventRoutingKey},
			{Queue: retryName, Exchange: cfg.RetryExchange, RoutingKey: workName},
			{Queue: dlqName, Exchange: cfg.DLQExchange, RoutingKey: workName},
		},
	}
}

// ConsumerFlow devuelve, como una Topology sin exchanges, las tres colas y
// los bindings del flujo de un consumidor, construidos por la misma función
// que usa Platform. Permite declarar o borrar un único flujo sobre exchanges
// ya declarados, por ejemplo en las pruebas de humo contra un broker real.
func ConsumerFlow(base, eventRoutingKey string, cfg config.Config) Topology {
	f := flow(base, eventRoutingKey, cfg)
	return Topology{Queues: f.queues, Bindings: f.bindings}
}

// flowBase devuelve el nombre base <consumidor>.<evento> de una cola de
// trabajo, es decir, su nombre sin el sufijo .q.
func flowBase(workName string) string {
	return strings.TrimSuffix(workName, workSuffix)
}
