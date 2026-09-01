// Package config carga la configuración de RabbitMQ y de la topología
// desde variables de entorno.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

// Config contiene la URL de conexión a RabbitMQ y los parámetros de la
// topología que este job declara.
type Config struct {
	RabbitMQURL      string
	MainExchange     string
	RetryExchange    string
	DLQExchange      string
	UnroutedExchange string
	RetryTTLMs       int32
}

// Load lee Config desde el entorno. RABBITMQ_URL es obligatoria; el resto
// de los valores tiene un default acorde a la topología estándar de la
// plataforma.
func Load() (Config, error) {
	cfg := Config{
		RabbitMQURL:      os.Getenv("RABBITMQ_URL"),
		MainExchange:     getEnvOrDefault("RABBITMQ_MAIN_EXCHANGE", "vvi.topic.exchange"),
		RetryExchange:    getEnvOrDefault("RABBITMQ_RETRY_EXCHANGE", "vvi.retry.exchange"),
		DLQExchange:      getEnvOrDefault("RABBITMQ_DLQ_EXCHANGE", "vvi.dlq.exchange"),
		UnroutedExchange: getEnvOrDefault("RABBITMQ_UNROUTED_EXCHANGE", "vvi.unrouted.exchange"),
	}

	if cfg.RabbitMQURL == "" {
		return Config{}, errors.New("config: la variable RABBITMQ_URL es obligatoria")
	}

	ttl, err := parseRetryTTL(getEnvOrDefault("RETRY_TTL_MS", "30000"))
	if err != nil {
		return Config{}, err
	}
	cfg.RetryTTLMs = ttl

	return cfg, nil
}

func parseRetryTTL(raw string) (int32, error) {
	v, err := strconv.ParseInt(raw, 10, 32)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("config: RETRY_TTL_MS debe ser un entero positivo, se recibió %q", raw)
	}
	return int32(v), nil
}

func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
