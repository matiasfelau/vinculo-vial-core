package config

import "testing"

func TestLoad_RequiresRabbitMQURL(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "")

	if _, err := Load(); err == nil {
		t.Fatal("se esperaba error al no definir RABBITMQ_URL")
	}
}

func TestLoad_Defaults(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
	t.Setenv("RABBITMQ_MAIN_EXCHANGE", "")
	t.Setenv("RABBITMQ_RETRY_EXCHANGE", "")
	t.Setenv("RABBITMQ_DLQ_EXCHANGE", "")
	t.Setenv("RABBITMQ_UNROUTED_EXCHANGE", "")
	t.Setenv("RETRY_TTL_MS", "")
	t.Setenv("ENV", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	if cfg.Env != "" {
		t.Errorf("Env = %q, se esperaba vacío por defecto", cfg.Env)
	}
	if cfg.MainExchange != "vvi.topic.exchange" {
		t.Errorf("MainExchange = %q, se esperaba vvi.topic.exchange", cfg.MainExchange)
	}
	if cfg.RetryExchange != "vvi.retry.exchange" {
		t.Errorf("RetryExchange = %q, se esperaba vvi.retry.exchange", cfg.RetryExchange)
	}
	if cfg.DLQExchange != "vvi.dlq.exchange" {
		t.Errorf("DLQExchange = %q, se esperaba vvi.dlq.exchange", cfg.DLQExchange)
	}
	if cfg.UnroutedExchange != "vvi.unrouted.exchange" {
		t.Errorf("UnroutedExchange = %q, se esperaba vvi.unrouted.exchange", cfg.UnroutedExchange)
	}
	if cfg.RetryTTLMs != 30000 {
		t.Errorf("RetryTTLMs = %d, se esperaba 30000", cfg.RetryTTLMs)
	}
}

func TestLoad_InvalidRetryTTL(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
	t.Setenv("RETRY_TTL_MS", "no-numerico")

	if _, err := Load(); err == nil {
		t.Fatal("se esperaba error por RETRY_TTL_MS inválido")
	}
}

func TestLoad_RetryTTLMustBePositive(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
	t.Setenv("RETRY_TTL_MS", "0")

	if _, err := Load(); err == nil {
		t.Fatal("se esperaba error por RETRY_TTL_MS no positivo")
	}
}

func TestLoad_PropagaEnv(t *testing.T) {
	t.Setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
	t.Setenv("ENV", "local")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
	if cfg.Env != "local" {
		t.Errorf("Env = %q, se esperaba local", cfg.Env)
	}
}
