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

func TestUS05_TopologiaCoherente(t *testing.T) {
	tp := US05(testConfig())
	if err := tp.Validate(); err != nil {
		t.Fatalf("la topología US-05 no es coherente: %v", err)
	}
}

func TestUS05_ExchangePrincipalTieneAlternateExchange(t *testing.T) {
	tp := US05(testConfig())

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

func TestUS05_ColaRetryTieneTTLDelExchangePrincipal(t *testing.T) {
	tp := US05(testConfig())

	for _, q := range tp.Queues {
		if q.Name != "alert.created.retry" {
			continue
		}
		if q.Arguments["x-message-ttl"] != int32(30000) {
			t.Errorf("x-message-ttl = %v, se esperaba 30000", q.Arguments["x-message-ttl"])
		}
		if q.Arguments["x-dead-letter-exchange"] != "vvi.topic.exchange" {
			t.Errorf("x-dead-letter-exchange = %v, se esperaba vvi.topic.exchange", q.Arguments["x-dead-letter-exchange"])
		}
		return
	}
	t.Fatal("alert.created.retry no fue declarada")
}
