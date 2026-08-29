package amqp

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/vinculo-vial/core-service/internal/topology"
)

type fakeChannel struct {
	exchangeErr map[string]error
	queueErr    map[string]error
	bindErr     map[string]error

	calls []string
}

func (f *fakeChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqplib.Table) error {
	f.calls = append(f.calls, "exchange:"+name)
	return f.exchangeErr[name]
}

func (f *fakeChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqplib.Table) (amqplib.Queue, error) {
	f.calls = append(f.calls, "queue:"+name)
	return amqplib.Queue{Name: name}, f.queueErr[name]
}

func (f *fakeChannel) QueueBind(name, key, exchange string, noWait bool, args amqplib.Table) error {
	f.calls = append(f.calls, "bind:"+name)
	return f.bindErr[name]
}

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func sampleTopology() topology.Topology {
	return topology.Topology{
		Exchanges: []topology.Exchange{
			{Name: "vvi.topic.exchange", Type: topology.ExchangeTopic, Durable: true},
		},
		Queues: []topology.Queue{
			{Name: "alert.created.q", Kind: topology.QueueKindWork, Durable: true},
		},
		Bindings: []topology.Binding{
			{Queue: "alert.created.q", Exchange: "vvi.topic.exchange", RoutingKey: "alert.created"},
		},
	}
}

func TestDeclarer_Apply_Idempotente(t *testing.T) {
	fake := &fakeChannel{}
	d := NewDeclarer(fake, silentLogger())
	tp := sampleTopology()

	if err := d.Apply(tp); err != nil {
		t.Fatalf("primera corrida: error inesperado: %v", err)
	}
	if err := d.Apply(tp); err != nil {
		t.Fatalf("segunda corrida sobre topología ya declarada: error inesperado: %v", err)
	}
}

func TestDeclarer_Apply_OrdenExchangesColasBindings(t *testing.T) {
	fake := &fakeChannel{}
	d := NewDeclarer(fake, silentLogger())

	if err := d.Apply(sampleTopology()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	want := []string{"exchange:vvi.topic.exchange", "queue:alert.created.q", "bind:alert.created.q"}
	if len(fake.calls) != len(want) {
		t.Fatalf("calls = %v, se esperaba %v", fake.calls, want)
	}
	for i, c := range want {
		if fake.calls[i] != c {
			t.Errorf("calls[%d] = %q, se esperaba %q", i, fake.calls[i], c)
		}
	}
}

func TestDeclarer_Apply_406EnCola(t *testing.T) {
	fake := &fakeChannel{
		queueErr: map[string]error{
			"alert.created.q": &amqplib.Error{Code: amqplib.PreconditionFailed, Reason: "PRECONDITION_FAILED", Server: true},
		},
	}
	d := NewDeclarer(fake, silentLogger())

	err := d.Apply(sampleTopology())
	if err == nil {
		t.Fatal("se esperaba error por 406 al redeclarar con argumentos distintos")
	}

	var amqpErr *amqplib.Error
	if !errors.As(err, &amqpErr) || amqpErr.Code != amqplib.PreconditionFailed {
		t.Fatalf("el error no envuelve el 406 original: %v", err)
	}

	for _, c := range fake.calls {
		if c == "bind:alert.created.q" {
			t.Fatal("no debería intentar el binding tras el 406 en la cola")
		}
	}
}

func TestDeclarer_Apply_ErrorGenericoEnvuelveCausaOriginal(t *testing.T) {
	original := errors.New("conexión perdida")
	fake := &fakeChannel{
		exchangeErr: map[string]error{
			"vvi.topic.exchange": original,
		},
	}
	d := NewDeclarer(fake, silentLogger())

	err := d.Apply(sampleTopology())
	if !errors.Is(err, original) {
		t.Fatalf("el error no envuelve la causa original: %v", err)
	}

	var amqpErr *amqplib.Error
	if errors.As(err, &amqpErr) {
		t.Fatal("un error genérico no debería reportarse como 406")
	}
}
