// Command core es el job de inicialización de infraestructura: declara la
// topología estándar de RabbitMQ de forma idempotente, la verifica y
// termina. No consume ni publica eventos de negocio, y no expone puertos
// ni HTTP: su salud es su código de salida.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"

	vviamqp "github.com/vinculo-vial/core-service/internal/amqp"
	"github.com/vinculo-vial/core-service/internal/topology"
	"github.com/vinculo-vial/core-service/internal/verify"
	"github.com/vinculo-vial/core-service/pkg/config"
)

const (
	brokerWaitTimeout   = 30 * time.Second
	brokerRetryInterval = 2 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(logger); err != nil {
		logger.Error("el job de inicialización de topología falló", "error", err)
		os.Exit(1)
	}

	logger.Info("topología declarada y verificada correctamente")
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
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

	t := topology.US05(cfg)
	if err := t.Validate(); err != nil {
		return fmt.Errorf("modelo de topología inconsistente: %w", err)
	}

	if err := declareTopology(conn, t, logger); err != nil {
		return err
	}

	if err := verifyTopology(conn, t); err != nil {
		return err
	}

	return nil
}

func declareTopology(conn *amqplib.Connection, t topology.Topology, logger *slog.Logger) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("abriendo canal de declaración: %w", err)
	}
	defer ch.Close()

	declarer := vviamqp.NewDeclarer(ch, logger)
	if err := declarer.Apply(t); err != nil {
		return fmt.Errorf("declarando topología: %w", err)
	}
	return nil
}

func verifyTopology(conn *amqplib.Connection, t topology.Topology) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("abriendo canal de verificación: %w", err)
	}
	defer ch.Close()

	verifier := verify.New(ch)
	if err := verifier.Verify(t); err != nil {
		return fmt.Errorf("verificando topología: %w", err)
	}
	return nil
}
