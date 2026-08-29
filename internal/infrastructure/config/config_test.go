package config

import "testing"

func TestLoad_RequiresRabbitMQURL(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "")

	if _, err := Load(); err == nil {
		t.Fatal("expected error when RABBITMQ_URL is unset")
	}
}

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	t.Setenv("RABBITMQ_EXCHANGE", "")
	t.Setenv("RABBITMQ_EXCHANGE_TYPE", "")
	t.Setenv("RABBITMQ_QUEUE", "")
	t.Setenv("RABBITMQ_ALERTS_ROUTING_KEY", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Exchange != "vvi.topic.exchange" {
		t.Errorf("Exchange = %q, want vvi.topic.exchange", cfg.Exchange)
	}
	if cfg.ExchangeType != "topic" {
		t.Errorf("ExchangeType = %q, want topic", cfg.ExchangeType)
	}
	if cfg.Queue != "core-service.alerts" {
		t.Errorf("Queue = %q, want core-service.alerts", cfg.Queue)
	}
	if cfg.AlertsRoutingKey != "alert.#" {
		t.Errorf("AlertsRoutingKey = %q, want alert.#", cfg.AlertsRoutingKey)
	}
}

func TestLoad_Overrides(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	t.Setenv("RABBITMQ_QUEUE", "custom.queue")
	t.Setenv("RABBITMQ_ALERTS_ROUTING_KEY", "alert.custom.#")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Queue != "custom.queue" {
		t.Errorf("Queue = %q, want custom.queue", cfg.Queue)
	}
	if cfg.AlertsRoutingKey != "alert.custom.#" {
		t.Errorf("AlertsRoutingKey = %q, want alert.custom.#", cfg.AlertsRoutingKey)
	}
}
