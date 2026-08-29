// Package rabbitmq is the driving adapter that consumes Event Envelopes
// from the platform's RabbitMQ broker and dispatches them into the
// application layer through the port.EventHandler interface.
package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/vinculo-vial/core-service/internal/application/port"
	"github.com/vinculo-vial/core-service/internal/domain/event"
	"github.com/vinculo-vial/core-service/internal/infrastructure/config"
)

// consumerTag identifies this service's subscription to RabbitMQ.
const consumerTag = "vvi-core-service"

// Consumer connects to RabbitMQ, binds the alerts queue to the platform's
// topic exchange, and hands each delivered event to an EventHandler.
type Consumer struct {
	cfg     config.Config
	handler port.EventHandler
	logger  *slog.Logger

	conn *amqp.Connection
	ch   *amqp.Channel
}

// NewConsumer builds a Consumer. Call Connect before Run.
func NewConsumer(cfg config.Config, handler port.EventHandler, logger *slog.Logger) *Consumer {
	return &Consumer{cfg: cfg, handler: handler, logger: logger}
}

// Connect dials RabbitMQ, declares the durable topic exchange and this
// service's queue, and binds the queue to the exchange on the configured
// alerts routing key.
func (c *Consumer) Connect() error {
	conn, err := amqp.Dial(c.cfg.RabbitMQURL)
	if err != nil {
		return fmt.Errorf("rabbitmq: dial: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("rabbitmq: open channel: %w", err)
	}

	if err := ch.ExchangeDeclare(
		c.cfg.Exchange,
		c.cfg.ExchangeType,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("rabbitmq: declare exchange %q: %w", c.cfg.Exchange, err)
	}

	q, err := ch.QueueDeclare(
		c.cfg.Queue,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("rabbitmq: declare queue %q: %w", c.cfg.Queue, err)
	}

	if err := ch.QueueBind(
		q.Name,
		c.cfg.AlertsRoutingKey,
		c.cfg.Exchange,
		false, // no-wait
		nil,
	); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("rabbitmq: bind queue %q to %q: %w", q.Name, c.cfg.AlertsRoutingKey, err)
	}

	if err := ch.Qos(10, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("rabbitmq: set qos: %w", err)
	}

	c.conn = conn
	c.ch = ch
	return nil
}

// Run consumes deliveries until ctx is cancelled or the channel closes.
func (c *Consumer) Run(ctx context.Context) error {
	deliveries, err := c.ch.ConsumeWithContext(
		ctx,
		c.cfg.Queue,
		consumerTag,
		false, // auto-ack: off, we ack/nack explicitly after handling
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("rabbitmq: consume from %q: %w", c.cfg.Queue, err)
	}

	c.logger.Info("consumer ready",
		"exchange", c.cfg.Exchange,
		"queue", c.cfg.Queue,
		"routingKey", c.cfg.AlertsRoutingKey,
	)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("rabbitmq: delivery channel closed")
			}
			c.process(ctx, d)
		}
	}
}

// process deserializes the delivery into an Event Envelope, dispatches it to
// the handler, and acks/nacks accordingly. Messages that don't conform to
// the platform's envelope contract are dropped rather than requeued, since
// retrying a malformed message can never succeed.
func (c *Consumer) process(ctx context.Context, d amqp.Delivery) {
	envelope, err := event.Parse(d.Body)
	if err != nil {
		c.logger.Error("discarding malformed event envelope", "error", err)
		_ = d.Nack(false, false)
		return
	}

	if err := c.handler.Handle(ctx, envelope); err != nil {
		c.logger.Error("event handler failed", "eventId", envelope.EventID, "error", err)
		_ = d.Nack(false, true)
		return
	}

	_ = d.Ack(false)
}

// Close releases the channel and connection.
func (c *Consumer) Close() error {
	var err error
	if c.ch != nil {
		if e := c.ch.Close(); e != nil {
			err = e
		}
	}
	if c.conn != nil {
		if e := c.conn.Close(); e != nil {
			err = e
		}
	}
	return err
}
