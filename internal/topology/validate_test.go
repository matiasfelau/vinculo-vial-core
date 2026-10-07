package topology

import "testing"

func baseValidTopology() Topology {
	return Topology{
		Exchanges: []Exchange{
			{Name: "vvi.topic.exchange", Type: ExchangeTopic, Durable: true},
		},
		Queues: []Queue{
			{
				Name: "alert.created.q", Kind: QueueKindWork, Durable: true,
				Arguments: map[string]any{
					"x-dead-letter-exchange":    "vvi.retry.exchange",
					"x-dead-letter-routing-key": "alert.created.q",
				},
			},
			{
				Name: "alert.created.retry", Kind: QueueKindRetry, Durable: true,
				Arguments: map[string]any{
					"x-dead-letter-exchange":    "",
					"x-dead-letter-routing-key": "alert.created.q",
				},
			},
			{Name: "alert.created.dlq", Kind: QueueKindDLQ, Durable: true},
		},
		Bindings: []Binding{
			{Queue: "alert.created.q", Exchange: "vvi.topic.exchange", RoutingKey: "alert.created"},
		},
	}
}

func TestValidate_TopologiaCoherente(t *testing.T) {
	if err := baseValidTopology().Validate(); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
}

func TestValidate_ColaDeTrabajoSinRetry(t *testing.T) {
	tp := baseValidTopology()
	tp.Queues = []Queue{tp.Queues[0], tp.Queues[2]} // sin .retry

	if err := tp.Validate(); err == nil {
		t.Fatal("se esperaba error: cola de trabajo sin .retry declarada")
	}
}

func TestValidate_ColaDeTrabajoSinDLQ(t *testing.T) {
	tp := baseValidTopology()
	tp.Queues = []Queue{tp.Queues[0], tp.Queues[1]} // sin .dlq

	if err := tp.Validate(); err == nil {
		t.Fatal("se esperaba error: cola de trabajo sin .dlq declarada")
	}
}

func TestValidate_BindingAExchangeNoDeclarado(t *testing.T) {
	tp := baseValidTopology()
	tp.Bindings = append(tp.Bindings, Binding{Queue: "alert.created.q", Exchange: "no.existe", RoutingKey: "x"})

	if err := tp.Validate(); err == nil {
		t.Fatal("se esperaba error: binding a un exchange no declarado")
	}
}

func TestValidate_BindingAColaNoDeclarada(t *testing.T) {
	tp := baseValidTopology()
	tp.Bindings = append(tp.Bindings, Binding{Queue: "no.existe.q", Exchange: "vvi.topic.exchange", RoutingKey: "x"})

	if err := tp.Validate(); err == nil {
		t.Fatal("se esperaba error: binding a una cola no declarada")
	}
}

func TestValidate_RetryConDeadLetterAlExchangePrincipal(t *testing.T) {
	tp := baseValidTopology()
	tp.Queues[1].Arguments["x-dead-letter-exchange"] = "vvi.topic.exchange"

	if err := tp.Validate(); err == nil {
		t.Fatal("se esperaba error: retry con dead-letter a vvi.topic.exchange")
	}
}

func TestValidate_RetryConDeadLetterPorRoutingKeyDelEvento(t *testing.T) {
	tp := baseValidTopology()
	tp.Queues[1].Arguments["x-dead-letter-routing-key"] = "alert.created"

	if err := tp.Validate(); err == nil {
		t.Fatal("se esperaba error: retry con dead-letter por routing key del evento")
	}
}

func TestValidate_RetrySinDeadLetterExchange(t *testing.T) {
	tp := baseValidTopology()
	delete(tp.Queues[1].Arguments, "x-dead-letter-exchange")

	if err := tp.Validate(); err == nil {
		t.Fatal("se esperaba error: retry sin x-dead-letter-exchange")
	}
}

func TestValidate_ColaDeTrabajoConDeadLetterPorRoutingKeyDelEvento(t *testing.T) {
	tp := baseValidTopology()
	tp.Queues[0].Arguments["x-dead-letter-routing-key"] = "alert.created"

	if err := tp.Validate(); err == nil {
		t.Fatal("se esperaba error: cola de trabajo con dead-letter por routing key del evento")
	}
}
