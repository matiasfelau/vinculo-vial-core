package topology

// Routing keys de los eventos que ligan cada cola de trabajo al exchange
// principal, según la tabla "Topología a declarar" del contrato.
const (
	// AlertCreatedRoutingKey es la routing key del evento publicado por
	// vinculo-vial-alert al validarse una alerta (GREEN/YELLOW).
	AlertCreatedRoutingKey = "alert.created"

	// SessionsRevokedRoutingKey es la routing key del evento publicado por
	// vinculo-vial-identity al revocarse las sesiones de un usuario.
	SessionsRevokedRoutingKey = "user.sessions.revoked"

	// AlertEvidenceUploadedRoutingKey es la routing key del evento publicado
	// por vinculo-vial-alert al subir la evidencia de una alerta.
	AlertEvidenceUploadedRoutingKey = "alert.evidence.uploaded"

	// IAEvidenceValidatedBindingKey es el patrón de binding de la cola de
	// Alert que consume el veredicto de IA: IA publica con la routing key
	// ia.evidence.validated.{green|yellow|red} y el patrón matchea las tres.
	IAEvidenceValidatedBindingKey = "ia.evidence.validated.*"

	// AlertRejectedRoutingKey es la routing key del evento publicado por
	// vinculo-vial-alert al rechazarse una alerta (veredicto RED).
	AlertRejectedRoutingKey = "alert.rejected"

	// AlertReportedRoutingKey es la routing key del evento publicado por
	// vinculo-vial-alert cuando el dueño reporta al emisor de una alerta.
	AlertReportedRoutingKey = "alert.reported"

	// UserStatusChangedRoutingKey es la routing key del evento publicado por
	// vinculo-vial-identity al cambiar el estado de la cuenta de un usuario.
	UserStatusChangedRoutingKey = "user.status.changed"

	// AlertExpiredRoutingKey es la routing key del evento publicado por
	// vinculo-vial-alert cuando una alerta vence sin veredicto (EXPIRED).
	AlertExpiredRoutingKey = "alert.expired"

	// AlertSeenRoutingKey es la routing key del evento publicado por
	// vinculo-vial-alert cuando el primer dueño abre el chat de una alerta.
	AlertSeenRoutingKey = "alert.seen"

	// VehiclePendingClaimedRoutingKey es la routing key del evento publicado
	// por vinculo-vial-vehicle cuando un usuario vincula una patente que tenía
	// alertas pendientes de entrega.
	VehiclePendingClaimedRoutingKey = "vehicle.pending.claimed"

	// AlertPendingDeliveredRoutingKey es la routing key del evento publicado
	// por vinculo-vial-alert al entregar en lote las alertas pendientes al
	// dueño que vinculó la patente.
	AlertPendingDeliveredRoutingKey = "alert.pending.delivered"

	// UserDeletedRoutingKey es la routing key del evento publicado por
	// vinculo-vial-identity al dar de baja la cuenta de un usuario. Tiene tres
	// consumidores (Alert, Vehicle y Benefits), cada uno con su propia cola de trabajo.
	UserDeletedRoutingKey = "user.deleted"

	// AuditAlertRecordedRoutingKey es la routing key del evento publicado por
	// vinculo-vial-alert, por outbox y en la TX de creación, con el registro
	// de auditoría de cada alerta. Su único consumidor es vinculo-vial-audit.
	AuditAlertRecordedRoutingKey = "audit.alert.recorded"

	// AuditAlertDeliveredRoutingKey es la routing key del evento publicado por
	// vinculo-vial-alert la primera vez que una alerta queda activa con al
	// menos un destinatario. Su único consumidor es vinculo-vial-audit.
	AuditAlertDeliveredRoutingKey = "audit.alert.delivered"

	// AuditAlertDiscardedRoutingKey es la routing key del evento publicado por
	// vinculo-vial-alert cuando una alerta ya no puede llegar a nadie. Su
	// único consumidor es vinculo-vial-audit.
	AuditAlertDiscardedRoutingKey = "audit.alert.discarded"

	// EmailRequestedRoutingKey es la routing key del evento publicado por
	// vinculo-vial-identity, por outbox, cuando hay que enviar un email
	// (verificación, recuperación de contraseña, aviso de cambio o aviso de
	// cuenta existente). Su único consumidor es vinculo-vial-notification.
	EmailRequestedRoutingKey = "email.requested"
)
