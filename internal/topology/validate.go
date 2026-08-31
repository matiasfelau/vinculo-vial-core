package topology

import (
	"fmt"
	"strings"
)

// Validate comprueba la coherencia interna de la topología: todo binding
// referencia una cola y un exchange declarados, y toda cola de trabajo
// tiene su .retry y su .dlq correspondientes declaradas.
func (t Topology) Validate() error {
	exchangeNames := make(map[string]bool, len(t.Exchanges))
	for _, e := range t.Exchanges {
		exchangeNames[e.Name] = true
	}

	queueNames := make(map[string]bool, len(t.Queues))
	for _, q := range t.Queues {
		queueNames[q.Name] = true
	}

	for _, b := range t.Bindings {
		if !queueNames[b.Queue] {
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

		base := strings.TrimSuffix(q.Name, ".q")
		retryName := base + ".retry"
		dlqName := base + ".dlq"

		if !queueNames[retryName] {
			return fmt.Errorf("topología: la cola de trabajo %q no tiene declarada su .retry (%q)", q.Name, retryName)
		}
		if !queueNames[dlqName] {
			return fmt.Errorf("topología: la cola de trabajo %q no tiene declarada su .dlq (%q)", q.Name, dlqName)
		}
	}

	return nil
}
