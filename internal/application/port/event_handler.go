// Package port declares the interfaces (ports) the application layer
// exposes to driving adapters, following a hexagonal architecture.
package port

import (
	"context"

	"github.com/vinculo-vial/core-service/internal/domain/event"
)

// EventHandler processes a validated Event Envelope consumed from the
// broker. Implementations live in the application layer; adapters (e.g. the
// RabbitMQ consumer) depend on this interface, never the other way around.
type EventHandler interface {
	Handle(ctx context.Context, envelope event.Envelope) error
}
