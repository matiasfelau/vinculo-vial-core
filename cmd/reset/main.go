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

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(logger); err != nil {
		logger.Error("el reset de topología falló", "error", err)
		os.Exit(1)
	}

	logger.Info("topología eliminada correctamente")
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	if err := reset.Guard(cfg.Env); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), brokerWaitTimeout+brokerRetryInterval)
	defer cancel()

	conn, err := vviamqp.WaitAndDial(ctx, func() (*amqplib.Connection, error) {
		return amqplib.Dial(cfg.RabbitMQURL)
	}, brokerWaitTimeout, brokerRetryInterval, logger)
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
	if err := resetter.Apply(topology.Notification(cfg)); err != nil {
		return fmt.Errorf("eliminando topología: %w", err)
	}

	return nil
}
