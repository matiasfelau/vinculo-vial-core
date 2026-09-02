package reset

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/vinculo-vial/core-service/internal/topology"
)

type fakeChannel struct {
	queueErr    map[string]error
	exchangeErr map[string]error

	calls []string
}

func (f *fakeChannel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	f.calls = append(f.calls, "queue:"+name)
	return 0, f.queueErr[name]
}

func (f *fakeChannel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	f.calls = append(f.calls, "exchange:"+name)
	return f.exchangeErr[name]
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
			{Name: "notification.alert-created.q", Kind: topology.QueueKindWork, Durable: true},
		},
	}
}

func TestResetter_Apply_BorraColasAntesQueExchanges(t *testing.T) {
	fake := &fakeChannel{}
	r := New(fake, silentLogger())

	if err := r.Apply(sampleTopology()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	want := []string{"queue:notification.alert-created.q", "exchange:vvi.topic.exchange"}
	if len(fake.calls) != len(want) {
		t.Fatalf("calls = %v, se esperaba %v", fake.calls, want)
	}
	for i, c := range want {
		if fake.calls[i] != c {
			t.Errorf("calls[%d] = %q, se esperaba %q", i, fake.calls[i], c)
		}
	}
}

func TestResetter_Apply_ErrorEnColaAbortaAntesDeLosExchanges(t *testing.T) {
	original := errors.New("cola con consumidores activos")
	fake := &fakeChannel{
		queueErr: map[string]error{"notification.alert-created.q": original},
	}
	r := New(fake, silentLogger())

	err := r.Apply(sampleTopology())
	if !errors.Is(err, original) {
		t.Fatalf("el error no envuelve la causa original: %v", err)
	}

	for _, c := range fake.calls {
		if c == "exchange:vvi.topic.exchange" {
			t.Fatal("no debería intentar borrar el exchange tras el error en la cola")
		}
	}
}

func TestResetter_Apply_ErrorEnExchange(t *testing.T) {
	original := errors.New("exchange con bindings activos")
	fake := &fakeChannel{
		exchangeErr: map[string]error{"vvi.topic.exchange": original},
	}
	r := New(fake, silentLogger())

	err := r.Apply(sampleTopology())
	if !errors.Is(err, original) {
		t.Fatalf("el error no envuelve la causa original: %v", err)
	}
}
