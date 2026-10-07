package topology

import (
	"fmt"
)

// Validate comprueba la coherencia interna de la topología: todo binding
// referencia una cola y un exchange declarados, y toda cola de trabajo W
// tiene su .retry y su .dlq declaradas y el ciclo de reintento ruteado por
// el nombre de W: W hace dead-letter con routing key W, y W.retry hace
// dead-letter al exchange default con routing key W, nunca a un exchange
// con bindings por routing key de evento como el principal.
func (t Topology) Validate() error {
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

		if err := validateRetryCycle(q, retry); err != nil {
			return err
		}
	}

	return nil
}

// validateRetryCycle comprueba que el dead-letter de la cola de trabajo y el
// de su .retry vuelvan solo a la cola de trabajo: el primero con routing key
// igual a su nombre, el segundo hacia el exchange default con esa misma
// routing key.
func validateRetryCycle(work, retry Queue) error {
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
