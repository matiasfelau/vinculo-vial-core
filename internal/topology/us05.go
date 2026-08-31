package topology

import "github.com/vinculo-vial/core-service/pkg/config"

// AlertCreatedRoutingKey es la routing key del evento publicado por
// vinculo-vial-alert al crearse una alerta anónima (US-05).
const AlertCreatedRoutingKey = "alert.created"

// US05 construye la topología de exchanges, colas y bindings que sostiene
// el flujo de alert.created descrito en US-05: la cola de trabajo, su
// ciclo de reintento con TTL, su cola de dead-letter final, y la red de
// contención de mensajes sin ruta mediante alternate-exchange.
func US05(cfg config.Config) Topology {
	return Topology{
		Exchanges: []Exchange{
			{
				Name:    cfg.MainExchange,
				Type:    ExchangeTopic,
				Durable: true,
				Arguments: map[string]any{
					"alternate-exchange": cfg.UnroutedExchange,
				},
			},
			{Name: cfg.RetryExchange, Type: ExchangeTopic, Durable: true},
			{Name: cfg.DLQExchange, Type: ExchangeTopic, Durable: true},
			{Name: cfg.UnroutedExchange, Type: ExchangeFanout, Durable: true},
		},
		Queues: []Queue{
			{
				Name:    "alert.created.q",
				Kind:    QueueKindWork,
				Durable: true,
				Arguments: map[string]any{
					"x-dead-letter-exchange":    cfg.RetryExchange,
					"x-dead-letter-routing-key": AlertCreatedRoutingKey,
				},
			},
			{
				Name:    "alert.created.retry",
				Kind:    QueueKindRetry,
				Durable: true,
				Arguments: map[string]any{
					"x-message-ttl":             cfg.RetryTTLMs,
					"x-dead-letter-exchange":    cfg.MainExchange,
					"x-dead-letter-routing-key": AlertCreatedRoutingKey,
				},
			},
			{
				Name:    "alert.created.dlq",
				Kind:    QueueKindDLQ,
				Durable: true,
			},
			{
				Name:    "vvi.unrouted.q",
				Kind:    QueueKindUnrouted,
				Durable: true,
			},
		},
		Bindings: []Binding{
			{Queue: "alert.created.q", Exchange: cfg.MainExchange, RoutingKey: AlertCreatedRoutingKey},
			{Queue: "alert.created.retry", Exchange: cfg.RetryExchange, RoutingKey: AlertCreatedRoutingKey},
			{Queue: "alert.created.dlq", Exchange: cfg.DLQExchange, RoutingKey: AlertCreatedRoutingKey},
			{Queue: "vvi.unrouted.q", Exchange: cfg.UnroutedExchange, RoutingKey: ""},
		},
	}
}
