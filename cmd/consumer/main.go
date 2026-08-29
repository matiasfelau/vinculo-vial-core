// Command consumer runs the Vínculo Vial Core Service's RabbitMQ consumer.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/vinculo-vial/core-service/internal/application/eventlog"
	"github.com/vinculo-vial/core-service/internal/infrastructure/config"
	"github.com/vinculo-vial/core-service/internal/infrastructure/messaging/rabbitmq"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	handler := eventlog.NewHandler(logger)
	consumer := rabbitmq.NewConsumer(cfg, handler, logger)

	if err := consumer.Connect(); err != nil {
		logger.Error("failed to connect to rabbitmq", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("consumer stopped unexpectedly", "error", err)
		os.Exit(1)
	}

	logger.Info("consumer shut down gracefully")
}
