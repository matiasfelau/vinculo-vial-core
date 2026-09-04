package topology

import (
	"testing"

	"github.com/vinculo-vial/core-service/pkg/config"
)

func testConfig() config.Config {
	return config.Config{
		RabbitMQURL:      "amqp://guest:guest@rabbitmq:5672/",
		MainExchange:     "vvi.topic.exchange",
		RetryExchange:    "vvi.retry.exchange",
		DLQExchange:      "vvi.dlq.exchange",
		UnroutedExchange: "vvi.unrouted.exchange",
		RetryTTLMs:       30000,
	}
}

func TestNotification_TopologiaCoherente(t *testing.T) {
	tp := Notification(testConfig())
	if err := tp.Validate(); err != nil {
		t.Fatalf("la topología de notification no es coherente: %v", err)
	}
}

func TestNotification_ExchangePrincipalTieneAlternateExchange(t *testing.T) {
	tp := Notification(testConfig())

	for _, e := range tp.Exchanges {
		if e.Name != "vvi.topic.exchange" {
			continue
		}
		if e.Arguments["alternate-exchange"] != "vvi.unrouted.exchange" {
			t.Fatalf("alternate-exchange = %v, se esperaba vvi.unrouted.exchange", e.Arguments["alternate-exchange"])
		}
		return
	}
	t.Fatal("vvi.topic.exchange no fue declarado")
}

func TestNotification_AlertCreatedRetryTieneTTLYDeadLetterAlExchangePrincipal(t *testing.T) {
	tp := Notification(testConfig())

	for _, q := range tp.Queues {
		if q.Name != "notification.alert-created.retry" {
			continue
		}
		if q.Arguments["x-message-ttl"] != int32(30000) {
			t.Errorf("x-message-ttl = %v, se esperaba 30000", q.Arguments["x-message-ttl"])
		}
		if q.Arguments["x-dead-letter-exchange"] != "vvi.topic.exchange" {
			t.Errorf("x-dead-letter-exchange = %v, se esperaba vvi.topic.exchange", q.Arguments["x-dead-letter-exchange"])
		}
		if q.Arguments["x-dead-letter-routing-key"] != AlertCreatedRoutingKey {
			t.Errorf("x-dead-letter-routing-key = %v, se esperaba %q", q.Arguments["x-dead-letter-routing-key"], AlertCreatedRoutingKey)
		}
		return
	}
	t.Fatal("notification.alert-created.retry no fue declarada")
}

func TestNotification_SessionsRevokedRetryTieneTTLYDeadLetterAlExchangePrincipal(t *testing.T) {
	tp := Notification(testConfig())

	for _, q := range tp.Queues {
		if q.Name != "notification.sessions-revoked.retry" {
			continue
		}
		if q.Arguments["x-message-ttl"] != int32(30000) {
			t.Errorf("x-message-ttl = %v, se esperaba 30000", q.Arguments["x-message-ttl"])
		}
		if q.Arguments["x-dead-letter-exchange"] != "vvi.topic.exchange" {
			t.Errorf("x-dead-letter-exchange = %v, se esperaba vvi.topic.exchange", q.Arguments["x-dead-letter-exchange"])
		}
		if q.Arguments["x-dead-letter-routing-key"] != SessionsRevokedRoutingKey {
			t.Errorf("x-dead-letter-routing-key = %v, se esperaba %q", q.Arguments["x-dead-letter-routing-key"], SessionsRevokedRoutingKey)
		}
		return
	}
	t.Fatal("notification.sessions-revoked.retry no fue declarada")
}

func TestNotification_ColasDeTrabajoEnrutanAlExchangePrincipal(t *testing.T) {
	tp := Notification(testConfig())

	want := map[string]string{
		"notification.alert-created.q":    AlertCreatedRoutingKey,
		"notification.sessions-revoked.q": SessionsRevokedRoutingKey,
	}

	found := map[string]bool{}
	for _, b := range tp.Bindings {
		routingKey, ok := want[b.Queue]
		if !ok {
			continue
		}
		if b.Exchange != "vvi.topic.exchange" {
			t.Errorf("binding de %q apunta a %q, se esperaba vvi.topic.exchange", b.Queue, b.Exchange)
		}
		if b.RoutingKey != routingKey {
			t.Errorf("binding de %q tiene routing key %q, se esperaba %q", b.Queue, b.RoutingKey, routingKey)
		}
		found[b.Queue] = true
	}

	for queue := range want {
		if !found[queue] {
			t.Errorf("no se encontró binding hacia el exchange principal para %q", queue)
		}
	}
}

func TestNotification_ColaSinRutaEstaLigadaAlAlternateExchange(t *testing.T) {
	tp := Notification(testConfig())

	for _, b := range tp.Bindings {
		if b.Queue != "vvi.unrouted.q" {
			continue
		}
		if b.Exchange != "vvi.unrouted.exchange" {
			t.Errorf("binding de vvi.unrouted.q apunta a %q, se esperaba vvi.unrouted.exchange", b.Exchange)
		}
		return
	}
	t.Fatal("vvi.unrouted.q no tiene binding declarado")
}
