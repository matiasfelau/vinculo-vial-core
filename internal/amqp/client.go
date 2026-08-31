// Package amqp encapsula el cliente AMQP de este job: la espera acotada de
// disponibilidad del broker y la declaración idempotente de la topología,
// con manejo explícito del 406 PRECONDITION_FAILED.
package amqp

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"
)

// DialFunc abre una conexión AMQP. Se recibe como parámetro para permitir
// testear la espera y el reintento sin necesidad de un broker real.
type DialFunc func() (*amqplib.Connection, error)

// WaitAndDial reintenta dial hasta lograr una conexión o hasta agotar
// maxWait. No es el reintento en loop que la declaración de topología
// prohíbe ante un 406: es la espera acotada de disponibilidad del broker al
// arrancar, necesaria porque el puerto AMQP puede tardar unos instantes más
// que el healthcheck del contenedor en aceptar conexiones.
func WaitAndDial(ctx context.Context, dial DialFunc, maxWait, retryInterval time.Duration, logger *slog.Logger) (*amqplib.Connection, error) {
	deadline := time.Now().Add(maxWait)

	var lastErr error
	for {
		conn, err := dial()
		if err == nil {
			return conn, nil
		}
		lastErr = err

		if !time.Now().Before(deadline) {
			return nil, fmt.Errorf("rabbitmq no respondió tras %s de espera: %w", maxWait, lastErr)
		}

		logger.Warn("rabbitmq todavía no disponible, reintentando conexión", "error", err)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryInterval):
		}
	}
}
