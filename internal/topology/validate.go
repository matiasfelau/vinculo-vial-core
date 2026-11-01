package topology

import (
	"fmt"

	"github.com/vinculo-vial/core-service/pkg/config"
)

// Validate comprueba la coherencia interna de la topología: todo binding
// referencia una cola y un exchange declarados, y toda cola de trabajo W
// tiene su .retry y su .dlq declaradas con el ciclo de reintento ruteado por
// el nombre de W:
//
//   - W hace dead-letter a cfg.RetryExchange con routing key W.
//   - W.retry está bindeada a cfg.RetryExchange por W y hace dead-letter al
//     exchange default con routing key W, nunca a un exchange con bindings
//     por routing key de evento como el principal.
//   - W.dlq está bindeada a cfg.DLQExchange por W.
//
// Los exchanges de retry y DLQ no tienen alternate-exchange: un binding que
// no coincide con W descarta el mensaje en silencio, por eso se exige cada
// uno.
func (t Topology) Validate(cfg config.Config) error {
	exchangeNames := make(map[string]bool, len(t.Exchanges))
	for _, e := range t.Exchanges {
		exchangeNames[e.Name] = true
	}

	queuesByName := make(map[string]Queue, len(t.Queues))
	for _, q := range t.Queues {
		queuesByName[q.Name] = q
	}

	for _, b := range t.Bindings {
		if _, ok := queuesByName[b.Queue]; !ok {
			return fmt.Errorf("topología: el binding hacia %q referencia la cola no declarada %q", b.Exchange, b.Queue)
		}
		if !exchangeNames[b.Exchange] {
			return fmt.Errorf("topología: el binding de %q referencia el exchange no declarado %q", b.Queue, b.Exchange)
		}
	}

	for _, q := range t.Queues {
		if q.Kind != QueueKindWork {
			continue
		}

		base := flowBase(q.Name)
		retryName := base + retrySuffix
		dlqName := base + dlqSuffix

		retry, ok := queuesByName[retryName]
		if !ok {
			return fmt.Errorf("topología: la cola de trabajo %q no tiene declarada su .retry (%q)", q.Name, retryName)
		}
		if _, ok := queuesByName[dlqName]; !ok {
			return fmt.Errorf("topología: la cola de trabajo %q no tiene declarada su .dlq (%q)", q.Name, dlqName)
		}

		if err := validateRetryCycle(q, retry, cfg); err != nil {
			return err
		}
		if !t.hasBinding(retryName, cfg.RetryExchange, q.Name) {
			return fmt.Errorf("topología: la cola %q debe estar bindeada a %q con routing key %q: sin ese binding el mensaje en retry se pierde en silencio",
				retryName, cfg.RetryExchange, q.Name)
		}
		if !t.hasBinding(dlqName, cfg.DLQExchange, q.Name) {
			return fmt.Errorf("topología: la cola %q debe estar bindeada a %q con routing key %q: sin ese binding el mensaje enviado a la DLQ se pierde en silencio",
				dlqName, cfg.DLQExchange, q.Name)
		}
	}

	return nil
}

// validateRetryCycle comprueba los argumentos de dead-letter de la cola de
// trabajo (hacia cfg.RetryExchange por su propio nombre) y de su .retry
// (hacia el exchange default por el nombre de la cola de trabajo).
func validateRetryCycle(work, retry Queue, cfg config.Config) error {
	if dlx := work.Arguments[argDeadLetterExchange]; dlx != cfg.RetryExchange {
		return fmt.Errorf("topología: la cola de trabajo %q debe hacer dead-letter a %q, no a %v", work.Name, cfg.RetryExchange, dlx)
	}
	if rk := work.Arguments[argDeadLetterRoutingKey]; rk != work.Name {
		return fmt.Errorf("topología: la cola de trabajo %q debe hacer dead-letter con routing key %q, tiene %v", work.Name, work.Name, rk)
	}

	dlx, ok := retry.Arguments[argDeadLetterExchange]
	if !ok || dlx != DefaultExchange {
		return fmt.Errorf("topología: la cola de retry %q debe hacer dead-letter al exchange default (%q), no a %v: "+
			"reentregaría el mensaje a todos los consumidores del evento", retry.Name, DefaultExchange, dlx)
	}
	if rk := retry.Arguments[argDeadLetterRoutingKey]; rk != work.Name {
		return fmt.Errorf("topología: la cola de retry %q debe hacer dead-letter con routing key %q, tiene %v", retry.Name, work.Name, rk)
	}

	return nil
}

// hasBinding indica si la topología declara el binding exacto
// (queue, exchange, routingKey).
func (t Topology) hasBinding(queue, exchange, routingKey string) bool {
	for _, b := range t.Bindings {
		if b.Queue == queue && b.Exchange == exchange && b.RoutingKey == routingKey {
			return true
		}
	}
	return false
}
