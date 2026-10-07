//go:build smoke

// Package smoke contiene pruebas de extremo a extremo contra un broker
// RabbitMQ real: confirman que la topología declarada por este job enruta
// efectivamente los eventos de dominio hacia la cola de Notification, algo
// que las pruebas unitarias con canal simulado no pueden demostrar. Quedan
// excluidas de "go test ./..." por el build tag "smoke": requieren
// RABBITMQ_URL apuntando a un broker disponible y el consumidor real de
// Notification detenido.
package smoke

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"

	vviamqp "github.com/vinculo-vial/core-service/internal/amqp"
	"github.com/vinculo-vial/core-service/internal/topology"
	"github.com/vinculo-vial/core-service/pkg/config"
)

const (
	brokerWaitTimeout   = 10 * time.Second
	brokerRetryInterval = time.Second
	deliveryTimeout     = 5 * time.Second
)

type destino struct {
	routingKey string
	queue      string
}

// TestRouting_LlegaALaCola declara la topología vigente contra el broker
// (idempotente, igual que cmd/core), verifica que no haya consumidores
// activos en las colas de trabajo y, para cada routing key del contrato,
// publica un mensaje en el exchange principal y confirma que llega a la cola
// de trabajo que la topología le asocia: ejercita el binding real, no una
// simulación.
func TestRouting_LlegaALaCola(t *testing.T) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("cargando configuración: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, cancel := context.WithTimeout(context.Background(), brokerWaitTimeout+brokerRetryInterval)
	defer cancel()

	conn, err := vviamqp.WaitAndDial(ctx, func() (*amqplib.Connection, error) {
		return amqplib.Dial(cfg.RabbitMQURL)
	}, brokerWaitTimeout, brokerRetryInterval, logger)
	if err != nil {
		t.Fatalf("conectando con rabbitmq: %v", err)
	}
	defer conn.Close()

	topo := topology.Platform(cfg)

	declareCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("abriendo canal de declaración: %v", err)
	}
	defer declareCh.Close()

	if err := vviamqp.NewDeclarer(declareCh, logger).Apply(topo); err != nil {
		t.Fatalf("declarando topología: %v", err)
	}

	routingKeys := []string{
		topology.SessionsRevokedRoutingKey,
		topology.AlertCreatedRoutingKey,
	}

	var destinos []destino
	for _, routingKey := range routingKeys {
		queue, ok := workQueueFor(topo, cfg.MainExchange, routingKey)
		if !ok {
			t.Fatalf("la topología no liga ninguna cola a %q en %q", routingKey, cfg.MainExchange)
		}

		q, err := declareCh.QueueDeclarePassive(queue, true, false, false, false, nil)
		if err != nil {
			t.Fatalf("verificando la cola %q: %v", queue, err)
		}
		if q.Consumers > 0 {
			t.Fatalf("la cola %q tiene %d consumidor(es) activo(s): detener vinculo-vial-notification antes de correr la prueba de humo", queue, q.Consumers)
		}

		destinos = append(destinos, destino{routingKey: routingKey, queue: queue})
	}

	for _, d := range destinos {
		t.Run(d.routingKey, func(t *testing.T) {
			consumeCh, err := conn.Channel()
			if err != nil {
				t.Fatalf("abriendo canal de consumo: %v", err)
			}
			defer consumeCh.Close()

			deliveries, err := consumeCh.Consume(d.queue, "", false, false, false, false, nil)
			if err != nil {
				t.Fatalf("suscribiendo a %q: %v", d.queue, err)
			}

			publishCh, err := conn.Channel()
			if err != nil {
				t.Fatalf("abriendo canal de publicación: %v", err)
			}
			defer publishCh.Close()

			returns := publishCh.NotifyReturn(make(chan amqplib.Return, 1))

			marker := fmt.Sprintf("smoke-%d-%s", time.Now().UnixNano(), d.routingKey)
			publishCtx, publishCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer publishCancel()

			err = publishCh.PublishWithContext(publishCtx, cfg.MainExchange, d.routingKey, true, false, amqplib.Publishing{
				ContentType:  "text/plain",
				DeliveryMode: amqplib.Persistent,
				Body:         []byte(marker),
			})
			if err != nil {
				t.Fatalf("publicando en %q: %v", cfg.MainExchange, err)
			}

			select {
			case ret := <-returns:
				t.Fatalf("se recibió basic.return inesperado para %q: %d %s", d.routingKey, ret.ReplyCode, ret.ReplyText)
			case msg, open := <-deliveries:
				if !open {
					t.Fatal("el canal de consumo se cerró antes de recibir el mensaje")
				}
				if string(msg.Body) != marker {
					msg.Nack(false, true)
					t.Fatalf("llegó un mensaje de otra corrida (%q), se esperaba %q", msg.Body, marker)
				}
				if err := msg.Ack(false); err != nil {
					t.Fatalf("confirmando el mensaje: %v", err)
				}
			case <-time.After(deliveryTimeout):
				t.Fatalf("no llegó ningún mensaje a %q dentro de %s", d.queue, deliveryTimeout)
			}
		})
	}
}

// workQueueFor busca, entre los bindings de topo, la cola ligada a exchange
// con la routing key dada: es la misma información que usaría cualquier
// consumidor real para decidir de qué cola leer.
func workQueueFor(topo topology.Topology, exchange, routingKey string) (string, bool) {
	for _, b := range topo.Bindings {
		if b.Exchange == exchange && b.RoutingKey == routingKey {
			return b.Queue, true
		}
	}
	return "", false
}
