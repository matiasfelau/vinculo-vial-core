// Command core es el job de inicialización de infraestructura: declara la
// topología estándar de RabbitMQ y el bucket de evidencia de S3 de forma
// idempotente, los verifica y termina. No consume ni publica eventos de
// negocio, no lee ni escribe objetos, y no expone puertos ni HTTP: su salud
// es su código de salida.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"

	vviamqp "github.com/vinculo-vial/core-service/internal/amqp"
	"github.com/vinculo-vial/core-service/internal/storage"
	"github.com/vinculo-vial/core-service/internal/topology"
	"github.com/vinculo-vial/core-service/internal/verify"
	"github.com/vinculo-vial/core-service/pkg/config"
)

const (
	brokerWaitTimeout   = 30 * time.Second
	brokerRetryInterval = 2 * time.Second

	// s3StageTimeout acota todo el tramo de S3 (espera, declaración y
	// verificación); cada operación individual se acota además con
	// S3_TIMEOUT_MS.
	s3StageTimeout = 2 * time.Minute

	s3WaitTimeout    = 30 * time.Second
	s3InitialBackoff = 500 * time.Millisecond
	s3MaxBackoff     = 5 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(logger); err != nil {
		logger.Error("el job de inicialización de topología falló", "error", err)
		os.Exit(1)
	}

	logger.Info("topología y bucket de evidencia declarados y verificados correctamente")
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	storageCfg, err := config.LoadStorage()
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

	t := topology.Platform(cfg)
	if err := t.Validate(cfg); err != nil {
		return fmt.Errorf("modelo de topología inconsistente: %w", err)
	}

	if err := declareTopology(conn, t, logger); err != nil {
		return err
	}

	if err := verifyTopology(conn, t); err != nil {
		return err
	}

	return provisionBucket(storageCfg, logger)
}

// provisionBucket declara el bucket de evidencia en S3 y verifica que su
// configuración coincida con lo declarado. Cualquier fallo es fatal.
func provisionBucket(cfg config.StorageConfig, logger *slog.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), s3StageTimeout)
	defer cancel()

	client, err := storage.NewClient(ctx, cfg)
	if err != nil {
		return fmt.Errorf("creando el cliente de S3: %w", err)
	}

	wait := storage.WaitOptions{Timeout: s3WaitTimeout, InitialBackoff: s3InitialBackoff, MaxBackoff: s3MaxBackoff}
	if err := storage.NewProvisioner(client, cfg, wait, logger).Apply(ctx); err != nil {
		return fmt.Errorf("declarando el bucket de evidencia: %w", err)
	}

	if err := verify.NewBucketVerifier(client, cfg).Verify(ctx); err != nil {
		return fmt.Errorf("verificando el bucket de evidencia: %w", err)
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
