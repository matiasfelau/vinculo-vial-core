// Package eventlog implements the initial EventHandler use case: log receipt
// of consumed events so the RabbitMQ wiring can be verified end to end.
package eventlog

import (
	"context"
	"log/slog"

	"github.com/vinculo-vial/core-service/internal/domain/event"
)

// Handler logs the event type and issuing actor of every consumed event.
type Handler struct {
	logger *slog.Logger
}

// NewHandler builds a Handler that writes to logger.
func NewHandler(logger *slog.Logger) *Handler {
	return &Handler{logger: logger}
}

// Handle satisfies port.EventHandler.
func (h *Handler) Handle(_ context.Context, envelope event.Envelope) error {
	h.logger.Info("event consumed",
		"eventId", envelope.EventID,
		"eventType", envelope.EventType,
		"actor", envelope.Actor.IssuerID,
	)
	return nil
}
