package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/vinculo-vial/core-service/internal/topology"
	"github.com/vinculo-vial/core-service/pkg/config"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func setEnv(t *testing.T, env string) {
	t.Helper()
	t.Setenv("RABBITMQ_URL", "amqp://usuario:clave@broker-de-prueba:5672/")
	t.Setenv("RABBITMQ_MAIN_EXCHANGE", "")
	t.Setenv("RABBITMQ_RETRY_EXCHANGE", "")
	t.Setenv("RABBITMQ_DLQ_EXCHANGE", "")
	t.Setenv("RABBITMQ_UNROUTED_EXCHANGE", "")
	t.Setenv("RETRY_TTL_MS", "")
	t.Setenv("ENV", env)
}

// fakeBroker registra lo que el reset borra y permite programar fallas.
type fakeBroker struct {
	deletedQueues    []string
	deletedExchanges []string

	failChannel bool
	queueErr    error
	exchangeErr error

	channelsOpened int
	channelsClosed int
	closed         bool
}

func (b *fakeBroker) Channel() (channel, error) {
	b.channelsOpened++
	if b.failChannel {
		return nil, errors.New("no se pudo abrir el canal")
	}
	return &fakeChannel{b: b}, nil
}

func (b *fakeBroker) Close() error {
	b.closed = true
	return nil
}

type fakeChannel struct{ b *fakeBroker }

func (c *fakeChannel) QueueDelete(name string, _, _, _ bool) (int, error) {
	if c.b.queueErr != nil {
		return 0, c.b.queueErr
	}
	c.b.deletedQueues = append(c.b.deletedQueues, name)
	return 0, nil
}

func (c *fakeChannel) ExchangeDelete(name string, _, _ bool) error {
	if c.b.exchangeErr != nil {
		return c.b.exchangeErr
	}
	c.b.deletedExchanges = append(c.b.deletedExchanges, name)
	return nil
}

func (c *fakeChannel) Close() error {
	c.b.channelsClosed++
	return nil
}

// connector devuelve un connector que entrega b, o falla con err, y cuenta
// cuántas veces se lo invocó.
func connectorFor(b *fakeBroker, err error) (connector, *int) {
	calls := 0
	return func(context.Context, config.Config, *slog.Logger) (broker, error) {
		calls++
		if err != nil {
			return nil, err
		}
		return b, nil
	}, &calls
}

func TestRun_BorraTodaLaTopologiaDeLaPlataformaEnEntornoLocal(t *testing.T) {
	setEnv(t, "local")
	b := &fakeBroker{}
	connect, _ := connectorFor(b, nil)

	if err := run(context.Background(), discardLogger(), connect); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	want := topology.Platform(cfg)
	if len(b.deletedQueues) != len(want.Queues) || len(b.deletedExchanges) != len(want.Exchanges) {
		t.Errorf("se borraron %d colas y %d exchanges; la plataforma declara %d y %d",
			len(b.deletedQueues), len(b.deletedExchanges), len(want.Queues), len(want.Exchanges))
	}
	if !b.closed || b.channelsOpened != 1 || b.channelsClosed != 1 {
		t.Errorf("conexión cerrada = %v, canales abiertos/cerrados = %d/%d; se esperaba cerrar todo",
			b.closed, b.channelsOpened, b.channelsClosed)
	}
}

func TestRun_NoSeEjecutaFueraDelEntornoLocalYNoConecta(t *testing.T) {
	for _, env := range []string{"production", "staging", ""} {
		t.Run("ENV="+env, func(t *testing.T) {
			setEnv(t, env)
			b := &fakeBroker{}
			connect, connects := connectorFor(b, nil)

			err := run(context.Background(), discardLogger(), connect)
			if err == nil {
				t.Fatal("el reset debe rechazarse fuera de ENV=local")
			}
			if *connects != 0 {
				t.Error("la guarda debe cortar antes de conectar con el broker")
			}
			if len(b.deletedQueues)+len(b.deletedExchanges) != 0 {
				t.Error("fuera de local no se debe borrar nada")
			}
		})
	}
}

func TestRun_SinRabbitMQURLFallaAntesDeConectar(t *testing.T) {
	setEnv(t, "local")
	t.Setenv("RABBITMQ_URL", "")
	connect, connects := connectorFor(&fakeBroker{}, nil)

	err := run(context.Background(), discardLogger(), connect)
	if err == nil || !strings.Contains(err.Error(), "RABBITMQ_URL") {
		t.Fatalf("error = %v, se esperaba mención a RABBITMQ_URL", err)
	}
	if *connects != 0 {
		t.Error("con configuración inválida no se debe abrir ninguna conexión")
	}
}

func TestRun_ElBrokerNoDisponibleSeReportaConLaCausa(t *testing.T) {
	setEnv(t, "local")
	cause := errors.New("connection refused")
	connect, _ := connectorFor(&fakeBroker{}, cause)

	err := run(context.Background(), discardLogger(), connect)
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), "conectando con rabbitmq") {
		t.Fatalf("error = %v, se esperaba 'conectando con rabbitmq' envolviendo la causa", err)
	}
}

func TestRun_FallaAlAbrirElCanalYCierraLaConexion(t *testing.T) {
	setEnv(t, "local")
	b := &fakeBroker{failChannel: true}
	connect, _ := connectorFor(b, nil)

	err := run(context.Background(), discardLogger(), connect)
	if err == nil || !strings.Contains(err.Error(), "abriendo canal de reset") {
		t.Fatalf("error = %v, se esperaba 'abriendo canal de reset'", err)
	}
	if !b.closed {
		t.Error("la conexión debe cerrarse aunque el canal no se haya abierto")
	}
}

func TestRun_UnErrorAlBorrarSeEnvuelveYSeCierraTodo(t *testing.T) {
	cause := errors.New("cola con consumidores activos")
	tests := []struct {
		name  string
		setup func(*fakeBroker)
	}{
		{"al borrar una cola", func(b *fakeBroker) { b.queueErr = cause }},
		{"al borrar un exchange", func(b *fakeBroker) { b.exchangeErr = cause }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setEnv(t, "local")
			b := &fakeBroker{}
			tt.setup(b)
			connect, _ := connectorFor(b, nil)

			err := run(context.Background(), discardLogger(), connect)
			if !errors.Is(err, cause) || !strings.Contains(err.Error(), "eliminando topología") {
				t.Fatalf("error = %v, se esperaba 'eliminando topología' envolviendo la causa", err)
			}
			if !b.closed || b.channelsClosed != 1 {
				t.Errorf("conexión cerrada = %v, canales cerrados = %d; se esperaba cerrar todo", b.closed, b.channelsClosed)
			}
		})
	}
}

func TestAmqpConnector_ElBrokerInaccesibleAgotaLaEsperaSinExponerCredenciales(t *testing.T) {
	cfg := config.Config{RabbitMQURL: "amqp://usuario:clave@127.0.0.1:1/"}
	connect := amqpConnector(40*time.Millisecond, 5*time.Millisecond)

	conn, err := connect(context.Background(), cfg, discardLogger())
	if err == nil {
		t.Fatalf("se esperaba error contra un puerto cerrado, se obtuvo la conexión %v", conn)
	}
	if conn != nil {
		t.Error("ante un error no debe devolverse una conexión")
	}
	if !strings.Contains(err.Error(), "no respondió") {
		t.Errorf("el error %q debería indicar que rabbitmq no respondió", err.Error())
	}
	if strings.Contains(err.Error(), "clave") {
		t.Errorf("el error %q no debe exponer las credenciales de la URL", err.Error())
	}
}
