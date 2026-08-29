package amqp

import (
	"errors"
	"fmt"
	"log/slog"

	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/vinculo-vial/core-service/internal/topology"
)

// Channel es el subconjunto del canal AMQP que el declarador necesita. Se
// abstrae como interfaz para permitir testearlo con un mock, sin un broker
// real.
type Channel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqplib.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqplib.Table) (amqplib.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqplib.Table) error
}

// Declarer aplica una topology.Topology sobre un canal AMQP.
type Declarer struct {
	ch     Channel
	logger *slog.Logger
}

// NewDeclarer construye un Declarer sobre el canal dado.
func NewDeclarer(ch Channel, logger *slog.Logger) *Declarer {
	return &Declarer{ch: ch, logger: logger}
}

// Apply declara exchanges, luego colas y luego bindings, en ese orden,
// porque una cola puede referenciar por dead-letter un exchange que debe
// existir de antemano, y un binding requiere que ambos extremos existan.
//
// La declaración es idempotente por construcción: RabbitMQ acepta sin
// efecto redeclarar un elemento existente con argumentos idénticos. Si los
// argumentos difieren, el broker responde 406 PRECONDITION_FAILED y cierra
// el canal; Apply detecta puntualmente ese código y devuelve un error
// explícito en lugar de reintentar o continuar como si la declaración
// hubiese tenido éxito.
func (d *Declarer) Apply(t topology.Topology) error {
	for _, e := range t.Exchanges {
		if err := d.declareExchange(e); err != nil {
			return err
		}
	}
	for _, q := range t.Queues {
		if err := d.declareQueue(q); err != nil {
			return err
		}
	}
	for _, b := range t.Bindings {
		if err := d.bind(b); err != nil {
			return err
		}
	}
	return nil
}

func (d *Declarer) declareExchange(e topology.Exchange) error {
	err := d.ch.ExchangeDeclare(e.Name, string(e.Type), e.Durable, false, false, false, amqplib.Table(e.Arguments))
	if err != nil {
		return wrapPreconditionFailed(err, "exchange", e.Name)
	}
	d.logger.Info("exchange declarado", "name", e.Name, "type", e.Type)
	return nil
}

func (d *Declarer) declareQueue(q topology.Queue) error {
	_, err := d.ch.QueueDeclare(q.Name, q.Durable, false, false, false, amqplib.Table(q.Arguments))
	if err != nil {
		return wrapPreconditionFailed(err, "cola", q.Name)
	}
	d.logger.Info("cola declarada", "name", q.Name, "kind", q.Kind)
	return nil
}

func (d *Declarer) bind(b topology.Binding) error {
	err := d.ch.QueueBind(b.Queue, b.RoutingKey, b.Exchange, false, nil)
	if err != nil {
		return wrapPreconditionFailed(err, "binding", fmt.Sprintf("%s -> %s", b.Queue, b.Exchange))
	}
	d.logger.Info("binding declarado", "queue", b.Queue, "exchange", b.Exchange, "routingKey", b.RoutingKey)
	return nil
}

// wrapPreconditionFailed distingue el 406 PRECONDITION_FAILED —que indica
// que el elemento ya existe con argumentos distintos— de cualquier otro
// error de declaración, y produce un mensaje accionable en cada caso.
func wrapPreconditionFailed(err error, kind, name string) error {
	var amqpErr *amqplib.Error
	if errors.As(err, &amqpErr) && amqpErr.Code == amqplib.PreconditionFailed {
		return fmt.Errorf("%s %q ya existe con argumentos distintos (406 PRECONDITION_FAILED); "+
			"los argumentos de una cola o exchange son inmutables, hay que borrarla para recrearla: %w", kind, name, err)
	}
	return fmt.Errorf("declarando %s %q: %w", kind, name, err)
}
