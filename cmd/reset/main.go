// Command reset elimina la topología declarada por cmd/core. Solo se
// ejecuta en un entorno local explícito (ENV=local); en cualquier otro
// caso termina con error antes de conectar al broker. No forma parte del
// arranque normal del sistema: es una herramienta manual de desarrollo.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"

	vviamqp "github.com/vinculo-vial/core-service/internal/amqp"
	"github.com/vinculo-vial/core-service/internal/reset"
	"github.com/vinculo-vial/core-service/internal/topology"
	"github.com/vinculo-vial/core-service/pkg/config"
)

const (
	brokerWaitTimeout   = 30 * time.Second
	brokerRetryInterval = 2 * time.Second
)

// channel es el canal AMQP con el que se borra la topología.
type channel interface {
	reset.Channel
	Close() error
}

// broker es la conexión AMQP del reset. Se abstrae para probar run sin un
// RabbitMQ real: *amqplib.Connection devuelve un *amqplib.Channel concreto,
// que no puede sustituirse por un doble.
type broker interface {
	Channel() (channel, error)
	Close() error
}

type amqpBroker struct{ conn *amqplib.Connection }

func (b amqpBroker) Channel() (channel, error) { return b.conn.Channel() }
func (b amqpBroker) Close() error              { return b.conn.Close() }

// connector abre la conexión con el broker.
type connector func(ctx context.Context, cfg config.Config, logger *slog.Logger) (broker, error)

// amqpConnector devuelve la conexión real con RabbitMQ: reintenta durante
// wait, cada retryInterval, hasta que el broker acepta conexiones.
func amqpConnector(wait, retryInterval time.Duration) connector {
	return func(ctx context.Context, cfg config.Config, logger *slog.Logger) (broker, error) {
		conn, err := vviamqp.WaitAndDial(ctx, func() (*amqplib.Connection, error) {
			return amqplib.Dial(cfg.RabbitMQURL)
		}, wait, retryInterval, logger)
		if err != nil {
			return nil, err
		}
		return amqpBroker{conn: conn}, nil
	}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(context.Background(), logger, amqpConnector(brokerWaitTimeout, brokerRetryInterval)); err != nil {
		logger.Error("el reset de topología falló", "error", err)
		os.Exit(1)
	}

	logger.Info("topología eliminada correctamente")
}

func run(ctx context.Context, logger *slog.Logger, connect connector) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	if err := reset.Guard(cfg.Env); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, brokerWaitTimeout+brokerRetryInterval)
	defer cancel()

	conn, err := connect(ctx, cfg, logger)
	if err != nil {
		return fmt.Errorf("conectando con rabbitmq: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("abriendo canal de reset: %w", err)
	}
	defer ch.Close()

	resetter := reset.New(ch, logger)
	if err := resetter.Apply(topology.Platform(cfg)); err != nil {
		return fmt.Errorf("eliminando topología: %w", err)
	}

	return nil
}
