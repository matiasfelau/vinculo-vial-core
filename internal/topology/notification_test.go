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

func TestNotification_RetryTieneTTLYVuelveSoloASuColaDeTrabajo(t *testing.T) {
	tp := Notification(testConfig())

	flows := map[string]string{
		"notification.alert-created.retry":    "notification.alert-created.q",
		"notification.sessions-revoked.retry": "notification.sessions-revoked.q",
	}

	found := map[string]bool{}
	for _, q := range tp.Queues {
		work, ok := flows[q.Name]
		if !ok {
			continue
		}
		found[q.Name] = true
		if q.Arguments["x-message-ttl"] != int32(30000) {
			t.Errorf("%s: x-message-ttl = %v, se esperaba 30000", q.Name, q.Arguments["x-message-ttl"])
		}
		if q.Arguments["x-dead-letter-exchange"] != "" {
			t.Errorf("%s: x-dead-letter-exchange = %v, se esperaba el exchange default", q.Name, q.Arguments["x-dead-letter-exchange"])
		}
		if q.Arguments["x-dead-letter-routing-key"] != work {
			t.Errorf("%s: x-dead-letter-routing-key = %v, se esperaba %q", q.Name, q.Arguments["x-dead-letter-routing-key"], work)
		}
	}

	for name := range flows {
		if !found[name] {
			t.Errorf("%s no fue declarada", name)
		}
	}
}

func TestNotification_ColasDeTrabajoHacenDeadLetterAlRetryPorSuNombre(t *testing.T) {
	tp := Notification(testConfig())

	for _, name := range []string{"notification.alert-created.q", "notification.sessions-revoked.q"} {
		found := false
		for _, q := range tp.Queues {
			if q.Name != name {
				continue
			}
			found = true
			if q.Arguments["x-dead-letter-exchange"] != "vvi.retry.exchange" {
				t.Errorf("%s: x-dead-letter-exchange = %v, se esperaba vvi.retry.exchange", name, q.Arguments["x-dead-letter-exchange"])
			}
			if q.Arguments["x-dead-letter-routing-key"] != name {
				t.Errorf("%s: x-dead-letter-routing-key = %v, se esperaba %q", name, q.Arguments["x-dead-letter-routing-key"], name)
			}
		}
		if !found {
			t.Errorf("%s no fue declarada", name)
		}
	}
}

func TestNotification_RetryYDLQEstanBindeadasPorNombreDeColaDeTrabajo(t *testing.T) {
	tp := Notification(testConfig())

	want := map[string]Binding{
		"notification.alert-created.retry":    {Exchange: "vvi.retry.exchange", RoutingKey: "notification.alert-created.q"},
		"notification.alert-created.dlq":      {Exchange: "vvi.dlq.exchange", RoutingKey: "notification.alert-created.q"},
		"notification.sessions-revoked.retry": {Exchange: "vvi.retry.exchange", RoutingKey: "notification.sessions-revoked.q"},
		"notification.sessions-revoked.dlq":   {Exchange: "vvi.dlq.exchange", RoutingKey: "notification.sessions-revoked.q"},
	}

	found := map[string]bool{}
	for _, b := range tp.Bindings {
		w, ok := want[b.Queue]
		if !ok {
			continue
		}
		found[b.Queue] = true
		if b.Exchange != w.Exchange || b.RoutingKey != w.RoutingKey {
			t.Errorf("binding de %q = (%q, %q), se esperaba (%q, %q)", b.Queue, b.Exchange, b.RoutingKey, w.Exchange, w.RoutingKey)
		}
	}

	for queue := range want {
		if !found[queue] {
			t.Errorf("no se encontró binding para %q", queue)
		}
	}
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
