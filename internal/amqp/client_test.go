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
