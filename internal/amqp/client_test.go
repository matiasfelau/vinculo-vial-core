package amqp

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"
)

func TestWaitAndDial_ReintentaYExpira(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	attempts := 0
	dial := func() (*amqplib.Connection, error) {
		attempts++
		return nil, errors.New("connection refused")
	}

	_, err := WaitAndDial(context.Background(), dial, 100*time.Millisecond, 20*time.Millisecond, logger)
	if err == nil {
		t.Fatal("se esperaba error tras agotar el tiempo de espera")
	}
	if attempts < 2 {
		t.Fatalf("se esperaban múltiples intentos de conexión, hubo %d", attempts)
	}
}

func TestWaitAndDial_RespetaCancelacionDeContexto(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dial := func() (*amqplib.Connection, error) {
		return nil, errors.New("connection refused")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := WaitAndDial(ctx, dial, time.Second, 10*time.Millisecond, logger)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("se esperaba context.Canceled, se obtuvo %v", err)
	}
}

func TestWaitAndDial_DevuelveLaConexionAlPrimerIntentoSinEsperar(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	want := &amqplib.Connection{}
	attempts := 0
	dial := func() (*amqplib.Connection, error) {
		attempts++
		return want, nil
	}

	got, err := WaitAndDial(context.Background(), dial, time.Second, time.Hour, logger)
	if err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
	if got != want {
		t.Error("WaitAndDial debe devolver la conexión que abrió dial")
	}
	if attempts != 1 {
		t.Errorf("hubo %d intentos, se esperaba 1", attempts)
	}
}

func TestWaitAndDial_SeRecuperaCuandoElBrokerYaEstaDisponible(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	want := &amqplib.Connection{}
	attempts := 0
	dial := func() (*amqplib.Connection, error) {
		attempts++
		if attempts < 3 {
			return nil, errors.New("connection refused")
		}
		return want, nil
	}

	got, err := WaitAndDial(context.Background(), dial, time.Second, time.Millisecond, logger)
	if err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
	if got != want || attempts != 3 {
		t.Errorf("conexión correcta = %v, intentos = %d; se esperaba la conexión tras 3 intentos", got == want, attempts)
	}
}

func TestWaitAndDial_ElErrorFinalEnvuelveLaUltimaCausa(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cause := errors.New("connection refused")
	dial := func() (*amqplib.Connection, error) { return nil, cause }

	_, err := WaitAndDial(context.Background(), dial, 30*time.Millisecond, 5*time.Millisecond, logger)
	if !errors.Is(err, cause) {
		t.Fatalf("el error %v debería envolver la última causa de la conexión", err)
	}
}
