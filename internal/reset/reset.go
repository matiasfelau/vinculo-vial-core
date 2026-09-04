package reset

import (
	"fmt"
	"log/slog"

	"github.com/vinculo-vial/core-service/internal/topology"
)

// Channel es el subconjunto del canal AMQP que el reset necesita. Se
// abstrae como interfaz para permitir testearlo con un mock, sin un broker
// real.
type Channel interface {
	QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error)
	ExchangeDelete(name string, ifUnused, noWait bool) error
}

// Resetter elimina una topology.Topology de un canal AMQP.
type Resetter struct {
	ch     Channel
	logger *slog.Logger
}

// New construye un Resetter sobre el canal dado.
func New(ch Channel, logger *slog.Logger) *Resetter {
	return &Resetter{ch: ch, logger: logger}
}

// Apply borra las colas y luego los exchanges de la topología dada, en ese
// orden, deteniéndose ante el primer error: es la operación inversa a
// Declarer.Apply y comparte su misma disciplina de fallo explícito, sin
// tolerar silenciosamente un elemento que no pudo borrarse.
func (r *Resetter) Apply(t topology.Topology) error {
	for _, q := range t.Queues {
		if err := r.deleteQueue(q); err != nil {
			return err
		}
	}
	for _, e := range t.Exchanges {
		if err := r.deleteExchange(e); err != nil {
			return err
		}
	}
	return nil
}

func (r *Resetter) deleteQueue(q topology.Queue) error {
	if _, err := r.ch.QueueDelete(q.Name, false, false, false); err != nil {
		return fmt.Errorf("borrando cola %q: %w", q.Name, err)
	}
	r.logger.Info("cola borrada", "name", q.Name)
	return nil
}

func (r *Resetter) deleteExchange(e topology.Exchange) error {
	if err := r.ch.ExchangeDelete(e.Name, false, false); err != nil {
		return fmt.Errorf("borrando exchange %q: %w", e.Name, err)
	}
	r.logger.Info("exchange borrado", "name", e.Name)
	return nil
}
