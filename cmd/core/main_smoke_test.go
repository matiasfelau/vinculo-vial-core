//go:build smoke

package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/vinculo-vial/core-service/pkg/config"
)

// El adaptador amqpBroker envuelve *amqplib.Connection para poder inyectar un
// doble en run; por eso solo se puede ejercitar contra un RabbitMQ real. Requiere
// RABBITMQ_URL, igual que el resto de las pruebas con el tag smoke.
func TestConectorRealAbreUnCanalYCierraContraUnBrokerReal(t *testing.T) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		t.Skip("RABBITMQ_URL no está definida")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b, err := defaultDeps().connect(ctx, config.Config{RabbitMQURL: url}, discardLogger())
	if err != nil {
		t.Fatalf("no se pudo conectar con el broker real: %v", err)
	}
	ch, err := b.Channel()
	if err != nil {
		t.Fatalf("la conexión real no abrió un canal: %v", err)
	}
	if err := ch.Close(); err != nil {
		t.Errorf("no se pudo cerrar el canal: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Errorf("no se pudo cerrar la conexión: %v", err)
	}
}
