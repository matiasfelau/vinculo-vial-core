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
	alertCreated := flow("notification.alert-created", AlertCreatedRoutingKey, cfg)
	sessionsRevoked := flow("notification.sessions-revoked", SessionsRevokedRoutingKey, cfg)

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

	for _, f := range []flowQueues{alertCreated, sessionsRevoked} {
		t.Queues = append(t.Queues, f.queues...)
		t.Bindings = append(t.Bindings, f.bindings...)
	}

	return t
}
