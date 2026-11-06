package topology

import "github.com/vinculo-vial/core-service/pkg/config"

// unroutedQueueName es la cola que recibe, vía alternate-exchange, los
// mensajes del exchange principal que no matchean ninguna cola.
const unroutedQueueName = "vvi.unrouted.q"

// flowSpec identifica el flujo de un consumidor contratado: su nombre base
// <consumidor>.<evento> y el patrón con que la cola de trabajo se liga al
// exchange principal.
type flowSpec struct {
	base            string
	eventRoutingKey string
}

// consumerFlows enumera, como datos, los flujos de todos los consumidores
// contratados de la plataforma. Agregar el de un caso de uso futuro es sumar
// una entrada acá.
func consumerFlows() []flowSpec {
	return []flowSpec{
		{base: "notification.alert-created", eventRoutingKey: AlertCreatedRoutingKey},
		{base: "notification.sessions-revoked", eventRoutingKey: SessionsRevokedRoutingKey},
		{base: "ia.alert-evidence-uploaded", eventRoutingKey: AlertEvidenceUploadedRoutingKey},
		{base: "alert.ia-evidence-validated", eventRoutingKey: IAEvidenceValidatedBindingKey},
		{base: "identity.alert-rejected", eventRoutingKey: AlertRejectedRoutingKey},
		{base: "identity.alert-reported", eventRoutingKey: AlertReportedRoutingKey},
		{base: "alert.user-status-changed", eventRoutingKey: UserStatusChangedRoutingKey},
		{base: "notification.alert-rejected", eventRoutingKey: AlertRejectedRoutingKey},
		{base: "notification.alert-expired", eventRoutingKey: AlertExpiredRoutingKey},
		{base: "notification.alert-seen", eventRoutingKey: AlertSeenRoutingKey},
		{base: "alert.vehicle-pending-claimed", eventRoutingKey: VehiclePendingClaimedRoutingKey},
		{base: "notification.alert-pending-delivered", eventRoutingKey: AlertPendingDeliveredRoutingKey},
		{base: "alert.user-deleted", eventRoutingKey: UserDeletedRoutingKey},
		{base: "vehicle.user-deleted", eventRoutingKey: UserDeletedRoutingKey},
		{base: "audit.alert-recorded", eventRoutingKey: AuditAlertRecordedRoutingKey},
		{base: "audit.alert-delivered", eventRoutingKey: AuditAlertDeliveredRoutingKey},
		{base: "audit.alert-discarded", eventRoutingKey: AuditAlertDiscardedRoutingKey},
	}
}

// Platform construye la topología completa de la plataforma: los cuatro
// exchanges, la cola de mensajes sin ruta (alternate-exchange del principal)
// y el flujo trabajo/retry/dlq de cada consumidor contratado.
func Platform(cfg config.Config) Topology {
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
			{Name: unroutedQueueName, Kind: QueueKindUnrouted, Durable: true},
		},
		Bindings: []Binding{
			{Queue: unroutedQueueName, Exchange: cfg.UnroutedExchange, RoutingKey: ""},
		},
	}

	for _, spec := range consumerFlows() {
		f := flow(spec.base, spec.eventRoutingKey, cfg)
		t.Queues = append(t.Queues, f.queues...)
		t.Bindings = append(t.Bindings, f.bindings...)
	}

	return t
}
