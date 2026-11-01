package topology

import (
	"strings"
	"testing"
)

// assertValidateError exige que Validate falle y que el error mencione la
// regla esperada, para que el test no pase por un fallo ajeno al caso.
func assertValidateError(t *testing.T, tp Topology, want string) {
	t.Helper()

	err := tp.Validate(testConfig())
	if err == nil {
		t.Fatalf("se esperaba un error que contenga %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %q, se esperaba que contenga %q", err.Error(), want)
	}
}

func baseValidTopology() Topology {
	return Topology{
		Exchanges: []Exchange{
			{Name: "vvi.topic.exchange", Type: ExchangeTopic, Durable: true},
			{Name: "vvi.retry.exchange", Type: ExchangeTopic, Durable: true},
			{Name: "vvi.dlq.exchange", Type: ExchangeTopic, Durable: true},
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
			{Queue: "alert.created.retry", Exchange: "vvi.retry.exchange", RoutingKey: "alert.created.q"},
			{Queue: "alert.created.dlq", Exchange: "vvi.dlq.exchange", RoutingKey: "alert.created.q"},
		},
	}
}

func TestValidate_TopologiaCoherente(t *testing.T) {
	if err := baseValidTopology().Validate(testConfig()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
}

func TestValidate_ColaDeTrabajoSinRetry(t *testing.T) {
	tp := baseValidTopology()
	tp.Queues = []Queue{tp.Queues[0], tp.Queues[2]} // sin .retry

	if err := tp.Validate(testConfig()); err == nil {
		t.Fatal("se esperaba error: cola de trabajo sin .retry declarada")
	}
}

func TestValidate_ColaDeTrabajoSinDLQ(t *testing.T) {
	tp := baseValidTopology()
	tp.Queues = []Queue{tp.Queues[0], tp.Queues[1]} // sin .dlq

	if err := tp.Validate(testConfig()); err == nil {
		t.Fatal("se esperaba error: cola de trabajo sin .dlq declarada")
	}
}

func TestValidate_BindingAExchangeNoDeclarado(t *testing.T) {
	tp := baseValidTopology()
	tp.Bindings = append(tp.Bindings, Binding{Queue: "alert.created.q", Exchange: "no.existe", RoutingKey: "x"})

	if err := tp.Validate(testConfig()); err == nil {
		t.Fatal("se esperaba error: binding a un exchange no declarado")
	}
}

func TestValidate_BindingAColaNoDeclarada(t *testing.T) {
	tp := baseValidTopology()
	tp.Bindings = append(tp.Bindings, Binding{Queue: "no.existe.q", Exchange: "vvi.topic.exchange", RoutingKey: "x"})

	if err := tp.Validate(testConfig()); err == nil {
		t.Fatal("se esperaba error: binding a una cola no declarada")
	}
}

func TestValidate_RetryConDeadLetterAlExchangePrincipal(t *testing.T) {
	tp := baseValidTopology()
	tp.Queues[1].Arguments["x-dead-letter-exchange"] = "vvi.topic.exchange"

	assertValidateError(t, tp, `cola de retry "alert.created.retry" debe hacer dead-letter al exchange default`)
}

func TestValidate_RetryConDeadLetterPorRoutingKeyDelEvento(t *testing.T) {
	tp := baseValidTopology()
	tp.Queues[1].Arguments["x-dead-letter-routing-key"] = "alert.created"

	assertValidateError(t, tp, `cola de retry "alert.created.retry" debe hacer dead-letter con routing key "alert.created.q"`)
}

func TestValidate_RetrySinDeadLetterExchange(t *testing.T) {
	tp := baseValidTopology()
	delete(tp.Queues[1].Arguments, "x-dead-letter-exchange")

	assertValidateError(t, tp, `cola de retry "alert.created.retry" debe hacer dead-letter al exchange default`)
}

func TestValidate_ColaDeTrabajoConDeadLetterPorRoutingKeyDelEvento(t *testing.T) {
	tp := baseValidTopology()
	tp.Queues[0].Arguments["x-dead-letter-routing-key"] = "alert.created"

	assertValidateError(t, tp, `cola de trabajo "alert.created.q" debe hacer dead-letter con routing key "alert.created.q"`)
}

func TestValidate_ColaDeTrabajoConDeadLetterAlExchangePrincipal(t *testing.T) {
	tp := baseValidTopology()
	tp.Queues[0].Arguments["x-dead-letter-exchange"] = "vvi.topic.exchange"

	assertValidateError(t, tp, `cola de trabajo "alert.created.q" debe hacer dead-letter a "vvi.retry.exchange"`)
}

func TestValidate_ColaDeTrabajoSinDeadLetterExchange(t *testing.T) {
	tp := baseValidTopology()
	delete(tp.Queues[0].Arguments, "x-dead-letter-exchange")

	assertValidateError(t, tp, `cola de trabajo "alert.created.q" debe hacer dead-letter a "vvi.retry.exchange"`)
}

func TestValidate_RetrySinBindingAlExchangeDeRetry(t *testing.T) {
	tp := baseValidTopology()
	tp.Bindings = []Binding{tp.Bindings[0], tp.Bindings[2]} // sin binding de W.retry

	assertValidateError(t, tp, `"alert.created.retry" debe estar bindeada a "vvi.retry.exchange"`)
}

func TestValidate_RetryBindeadaConRoutingKeyDelEvento(t *testing.T) {
	tp := baseValidTopology()
	tp.Bindings[1].RoutingKey = "alert.created"

	assertValidateError(t, tp, `"alert.created.retry" debe estar bindeada a "vvi.retry.exchange"`)
}

func TestValidate_RetryBindeadaAOtroExchange(t *testing.T) {
	tp := baseValidTopology()
	tp.Bindings[1].Exchange = "vvi.dlq.exchange"

	assertValidateError(t, tp, `"alert.created.retry" debe estar bindeada a "vvi.retry.exchange"`)
}

func TestValidate_DLQSinBindingAlExchangeDeDLQ(t *testing.T) {
	tp := baseValidTopology()
	tp.Bindings = []Binding{tp.Bindings[0], tp.Bindings[1]} // sin binding de W.dlq

	assertValidateError(t, tp, `"alert.created.dlq" debe estar bindeada a "vvi.dlq.exchange"`)
}

func TestValidate_DLQBindeadaConRoutingKeyDelEvento(t *testing.T) {
	tp := baseValidTopology()
	tp.Bindings[2].RoutingKey = "alert.created"

	assertValidateError(t, tp, `"alert.created.dlq" debe estar bindeada a "vvi.dlq.exchange"`)
}

func TestValidate_DLQBindeadaAOtroExchange(t *testing.T) {
	tp := baseValidTopology()
	tp.Bindings[2].Exchange = "vvi.retry.exchange"

	assertValidateError(t, tp, `"alert.created.dlq" debe estar bindeada a "vvi.dlq.exchange"`)
}
