// Package config loads runtime configuration for the service's
// infrastructure adapters from environment variables.
package config

import (
	"errors"
	"os"
)

// Config holds the RabbitMQ connection and topology settings.
type Config struct {
	RabbitMQURL      string
	Exchange         string
	ExchangeType     string
	Queue            string
	AlertsRoutingKey string
}

// Load reads Config from the environment. RABBITMQ_URL is required; every
// other setting falls back to a sane default matching the platform's
// documented topic exchange.
func Load() (Config, error) {
	cfg := Config{
		RabbitMQURL:      os.Getenv("RABBITMQ_URL"),
		Exchange:         getEnvOrDefault("RABBITMQ_EXCHANGE", "vvi.topic.exchange"),
		ExchangeType:     getEnvOrDefault("RABBITMQ_EXCHANGE_TYPE", "topic"),
		Queue:            getEnvOrDefault("RABBITMQ_QUEUE", "core-service.alerts"),
		AlertsRoutingKey: getEnvOrDefault("RABBITMQ_ALERTS_ROUTING_KEY", "alert.#"),
	}

	if cfg.RabbitMQURL == "" {
		return Config{}, errors.New("config: RABBITMQ_URL is required")
	}

	return cfg, nil
}

func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
