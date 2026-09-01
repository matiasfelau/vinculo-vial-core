// Package verify comprueba, mediante declaración pasiva, que la topología
// aplicada por internal/amqp existe realmente en el broker.
package verify

import (
	"fmt"

	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/vinculo-vial/core-service/internal/topology"
)

// Channel es el subconjunto del canal AMQP necesario para verificar
// mediante declaración pasiva. Se abstrae como interfaz para permitir
// testearlo con un mock, sin un broker real.
type Channel interface {
	ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqplib.Table) error
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqplib.Table) (amqplib.Queue, error)
}

// Verifier confirma la existencia de una topology.Topology en el broker.
type Verifier struct {
	ch Channel
}

// New construye un Verifier sobre el canal dado.
func New(ch Channel) *Verifier {
	return &Verifier{ch: ch}
}

// Verify declara pasivamente cada exchange y cada cola de la topología. Una
// declaración pasiva no crea nada: falla si el elemento no existe, lo que
// confirma que la declaración activa (Declarer.Apply) realmente se ejecutó
// contra el broker y no solo contra el modelo en memoria.
func (v *Verifier) Verify(t topology.Topology) error {
	for _, e := range t.Exchanges {
		if err := v.ch.ExchangeDeclarePassive(e.Name, string(e.Type), e.Durable, false, false, false, nil); err != nil {
			return fmt.Errorf("el exchange %q no existe en el broker: %w", e.Name, err)
		}
	}
	for _, q := range t.Queues {
		if _, err := v.ch.QueueDeclarePassive(q.Name, q.Durable, false, false, false, nil); err != nil {
			return fmt.Errorf("la cola %q no existe en el broker: %w", q.Name, err)
		}
	}
	return nil
}
