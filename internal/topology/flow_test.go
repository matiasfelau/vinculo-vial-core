package topology

import (
	"reflect"
	"testing"
)

const (
	testBase       = "consumer.some-event"
	testWork       = "consumer.some-event.q"
	testRetry      = "consumer.some-event.retry"
	testDLQ        = "consumer.some-event.dlq"
	testEventKey   = "some.event"
	mainExchange   = "vvi.topic.exchange"
	retryExchange  = "vvi.retry.exchange"
	dlqExchange    = "vvi.dlq.exchange"
	expectedTTLMs  = int32(30000)
	expectedQueues = 3
)

func queueNamed(t *testing.T, f flowQueues, name string) Queue {
	t.Helper()
	for _, q := range f.queues {
		if q.Name == name {
			return q
		}
	}
	t.Fatalf("el flujo no declara la cola %q", name)
	return Queue{}
}

func TestFlow_DeclaraTresColasDurables(t *testing.T) {
	f := flow(testBase, testEventKey, testConfig())

	if len(f.queues) != expectedQueues {
		t.Fatalf("el flujo declara %d colas, se esperaban %d", len(f.queues), expectedQueues)
	}

	kinds := map[string]QueueKind{testWork: QueueKindWork, testRetry: QueueKindRetry, testDLQ: QueueKindDLQ}
	for name, kind := range kinds {
		q := queueNamed(t, f, name)
		if q.Kind != kind {
			t.Errorf("%q tiene kind %q, se esperaba %q", name, q.Kind, kind)
		}
		if !q.Durable {
			t.Errorf("%q no es durable", name)
		}
	}
}

func TestFlow_ColaDeTrabajoHaceDeadLetterAlRetryPorSuNombre(t *testing.T) {
	f := flow(testBase, testEventKey, testConfig())

	want := map[string]any{
		"x-dead-letter-exchange":    retryExchange,
		"x-dead-letter-routing-key": testWork,
	}
	if got := queueNamed(t, f, testWork).Arguments; !reflect.DeepEqual(got, want) {
		t.Fatalf("argumentos de %q = %v, se esperaba %v", testWork, got, want)
	}
}

func TestFlow_RetryVuelveAlExchangeDefaultPorElNombreDeLaColaDeTrabajo(t *testing.T) {
	f := flow(testBase, testEventKey, testConfig())

	want := map[string]any{
		"x-message-ttl":             expectedTTLMs,
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": testWork,
	}
	got := queueNamed(t, f, testRetry).Arguments
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argumentos de %q = %v, se esperaba %v", testRetry, got, want)
	}
	if got["x-dead-letter-exchange"] == mainExchange {
		t.Fatalf("%q no puede hacer dead-letter a %q", testRetry, mainExchange)
	}
	if got["x-dead-letter-routing-key"] == testEventKey {
		t.Fatalf("%q no puede hacer dead-letter por la routing key del evento", testRetry)
	}
}

func TestFlow_DLQNoLlevaArgumentos(t *testing.T) {
	f := flow(testBase, testEventKey, testConfig())

	if args := queueNamed(t, f, testDLQ).Arguments; len(args) != 0 {
		t.Fatalf("la DLQ no debe llevar argumentos, tiene %v", args)
	}
}

func TestFlow_BindingsExactos(t *testing.T) {
	f := flow(testBase, testEventKey, testConfig())

	want := []Binding{
		{Queue: testWork, Exchange: mainExchange, RoutingKey: testEventKey},
		{Queue: testRetry, Exchange: retryExchange, RoutingKey: testWork},
		{Queue: testDLQ, Exchange: dlqExchange, RoutingKey: testWork},
	}
	if !reflect.DeepEqual(f.bindings, want) {
		t.Fatalf("bindings = %v, se esperaba %v", f.bindings, want)
	}
}

func TestFlow_DosConsumidoresDelMismoEventoNoComparteRetryNiDLQ(t *testing.T) {
	cfg := testConfig()
	a := flow("consumer-a.some-event", testEventKey, cfg)
	b := flow("consumer-b.some-event", testEventKey, cfg)

	retryKeys := map[string]string{}
	for _, f := range []flowQueues{a, b} {
		for _, bd := range f.bindings {
			if bd.Exchange == cfg.RetryExchange || bd.Exchange == cfg.DLQExchange {
				key := bd.Exchange + "|" + bd.RoutingKey
				if owner, dup := retryKeys[key]; dup {
					t.Fatalf("%q y %q comparten el binding %q", owner, bd.Queue, key)
				}
				retryKeys[key] = bd.Queue
			}
		}
	}
}

func TestConsumerFlow_EsElMismoFlujoQueUsaPlatform(t *testing.T) {
	cfg := testConfig()
	f := flow(testBase, testEventKey, cfg)

	got := ConsumerFlow(testBase, testEventKey, cfg)

	if !reflect.DeepEqual(got.Queues, f.queues) || !reflect.DeepEqual(got.Bindings, f.bindings) {
		t.Fatalf("ConsumerFlow = %+v, se esperaba el flujo de flow(): %+v", got, f)
	}
	if len(got.Exchanges) != 0 {
		t.Fatalf("ConsumerFlow no debe declarar exchanges, declara %v", got.Exchanges)
	}
}
