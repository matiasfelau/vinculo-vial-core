package topology

import "github.com/vinculo-vial/core-service/pkg/config"

// AlertCreatedRoutingKey es la routing key del evento publicado por
// vinculo-vial-alert al crearse una alerta anónima.
const AlertCreatedRoutingKey = "alert.created"

// SessionsRevokedRoutingKey es la routing key del evento publicado por
// vinculo-vial-identity al revocarse las sesiones de un usuario.
const SessionsRevokedRoutingKey = "user.sessions.revoked"

// Notification construye la topología completa que sostiene a
// vinculo-vial-notification como consumidor: la cola de trabajo de
// alert.created y la de user.sessions.revoked, cada una con su ciclo de
// reintento y su cola de dead-letter, más la red de contención de mensajes
// sin ruta mediante alternate-exchange.
func Notification(cfg config.Config) Topology {
	alertCreated := notificationFlow("notification.alert-created", AlertCreatedRoutingKey, cfg)
	sessionsRevoked := notificationFlow("notification.sessions-revoked", SessionsRevokedRoutingKey, cfg)

	t := Topology{
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
			{Name: "vvi.unrouted.q", Kind: QueueKindUnrouted, Durable: true},
		},
		Bindings: []Binding{
			{Queue: "vvi.unrouted.q", Exchange: cfg.UnroutedExchange, RoutingKey: ""},
		},
	}

	for _, flow := range []notificationFlowQueues{alertCreated, sessionsRevoked} {
		t.Queues = append(t.Queues, flow.queues...)
		t.Bindings = append(t.Bindings, flow.bindings...)
	}

	return t
}

// notificationFlowQueues agrupa la cola de trabajo, su .retry y su .dlq de
// un evento consumido por vinculo-vial-notification, junto con los
// bindings que las conectan a los exchanges principal, de reintento y de
// dead-letter.
type notificationFlowQueues struct {
	queues   []Queue
	bindings []Binding
}

// notificationFlow construye el ciclo de trabajo/retry/dlq de un evento
// según la convención de nombres <consumidor>.<evento>: base ya incluye el
// consumidor y el evento (por ejemplo "notification.alert-created"), y esta
// función agrega los sufijos .q, .retry y .dlq.
func notificationFlow(base, routingKey string, cfg config.Config) notificationFlowQueues {
	workName := base + ".q"
	retryName := base + ".retry"
	dlqName := base + ".dlq"

	return notificationFlowQueues{
		queues: []Queue{
			{
				Name:    workName,
				Kind:    QueueKindWork,
				Durable: true,
				Arguments: map[string]any{
					"x-dead-letter-exchange":    cfg.RetryExchange,
					"x-dead-letter-routing-key": routingKey,
				},
			},
			{
				Name:    retryName,
				Kind:    QueueKindRetry,
				Durable: true,
				Arguments: map[string]any{
					"x-message-ttl":             cfg.RetryTTLMs,
					"x-dead-letter-exchange":    cfg.MainExchange,
					"x-dead-letter-routing-key": routingKey,
				},
			},
			{Name: dlqName, Kind: QueueKindDLQ, Durable: true},
		},
		bindings: []Binding{
			{Queue: workName, Exchange: cfg.MainExchange, RoutingKey: routingKey},
			{Queue: retryName, Exchange: cfg.RetryExchange, RoutingKey: routingKey},
			{Queue: dlqName, Exchange: cfg.DLQExchange, RoutingKey: routingKey},
		},
	}
}
