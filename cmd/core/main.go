// Command core es el job de inicialización de infraestructura: declara la
// topología estándar de RabbitMQ y los buckets de evidencia y de Audit de S3
// de forma idempotente, los verifica y termina. No consume ni publica eventos de
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

// s3API reúne lo que el job necesita del cliente S3: declarar (storage.API) y
// verificar (verify.BucketAPI). *s3.Client lo cumple; las pruebas inyectan un
// doble.
type s3API interface {
	storage.API
	verify.BucketAPI
}

// channel es el canal AMQP con el que se declara y se verifica la topología.
type channel interface {
	vviamqp.Channel
	verify.Channel
	Close() error
}

// broker es la conexión AMQP del job. Se abstrae para probar run sin un
// RabbitMQ real: *amqplib.Connection devuelve un *amqplib.Channel concreto,
// que no puede sustituirse por un doble.
type broker interface {
	Channel() (channel, error)
	Close() error
}

type amqpBroker struct{ conn *amqplib.Connection }

func (b amqpBroker) Channel() (channel, error) { return b.conn.Channel() }
func (b amqpBroker) Close() error              { return b.conn.Close() }

// deps agrupa las dependencias externas de run: la conexión con el broker y
// el cliente de S3.
type deps struct {
	connect func(ctx context.Context, cfg config.Config, logger *slog.Logger) (broker, error)
	newS3   func(ctx context.Context, cfg config.StorageConfig) (s3API, error)
}

func defaultDeps() deps {
	return deps{
		connect: amqpConnector(brokerWaitTimeout, brokerRetryInterval),
		newS3: func(ctx context.Context, cfg config.StorageConfig) (s3API, error) {
			return storage.NewClient(ctx, cfg)
		},
	}
}

// amqpConnector devuelve la conexión real con RabbitMQ: reintenta durante
// wait, cada retryInterval, hasta que el broker acepta conexiones.
func amqpConnector(wait, retryInterval time.Duration) func(context.Context, config.Config, *slog.Logger) (broker, error) {
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

	if err := run(context.Background(), logger, defaultDeps()); err != nil {
		logger.Error("el job de inicialización de topología falló", "error", err)
		os.Exit(1)
	}

	logger.Info("topología y buckets de evidencia y de Audit declarados y verificados correctamente")
}

func run(ctx context.Context, logger *slog.Logger, d deps) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	storageCfg, err := config.LoadStorage()
	if err != nil {
		return err
	}

	brokerCtx, cancel := context.WithTimeout(ctx, brokerWaitTimeout+brokerRetryInterval)
	defer cancel()

	conn, err := d.connect(brokerCtx, cfg, logger)
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

	return provisionBuckets(ctx, storageCfg, d.newS3, logger)
}

// provisionBuckets declara en S3 el bucket de evidencia y el de Audit y
// verifica que la configuración de cada uno coincida con lo declarado.
// Cualquier fallo es fatal.
func provisionBuckets(ctx context.Context, cfg config.StorageConfig, newS3 func(context.Context, config.StorageConfig) (s3API, error), logger *slog.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, s3StageTimeout)
	defer cancel()

	client, err := newS3(ctx, cfg)
	if err != nil {
		return fmt.Errorf("creando el cliente de S3: %w", err)
	}

	wait := storage.WaitOptions{Timeout: s3WaitTimeout, InitialBackoff: s3InitialBackoff, MaxBackoff: s3MaxBackoff}
	for _, spec := range []storage.BucketSpec{storage.EvidenceSpec(cfg), storage.AuditSpec(cfg)} {
		if err := storage.NewProvisioner(client, spec, cfg, wait, logger).Apply(ctx); err != nil {
			return fmt.Errorf("declarando el bucket %s: %w", spec.Label, err)
		}

		if err := verify.NewBucketVerifier(client, spec, cfg).Verify(ctx); err != nil {
			return fmt.Errorf("verificando el bucket %s: %w", spec.Label, err)
		}
	}
	return nil
}

func declareTopology(conn broker, t topology.Topology, logger *slog.Logger) error {
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

func verifyTopology(conn broker, t topology.Topology) error {
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
