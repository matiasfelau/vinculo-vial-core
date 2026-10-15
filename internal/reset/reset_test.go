package reset

import (
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/vinculo-vial/core-service/internal/topology"
	"github.com/vinculo-vial/core-service/pkg/config"
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

func TestResetter_Apply_CubreTodaLaTopologiaDeLaPlataforma(t *testing.T) {
	cfg := config.Config{
		MainExchange:     "vvi.topic.exchange",
		RetryExchange:    "vvi.retry.exchange",
		DLQExchange:      "vvi.dlq.exchange",
		UnroutedExchange: "vvi.unrouted.exchange",
		RetryTTLMs:       30000,
	}
	platform := topology.Platform(cfg)
	fake := &fakeChannel{}

	if err := New(fake, silentLogger()).Apply(platform); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}

	wantCalls := len(platform.Queues) + len(platform.Exchanges)
	if len(fake.calls) != wantCalls {
		t.Fatalf("el reset hizo %d borrados, se esperaban %d (una por cola y por exchange)", len(fake.calls), wantCalls)
	}

	deleted := map[string]bool{}
	lastQueue, firstExchange := -1, len(fake.calls)
	for i, c := range fake.calls {
		deleted[c] = true
		if strings.HasPrefix(c, "queue:") {
			lastQueue = i
		} else if i < firstExchange {
			firstExchange = i
		}
	}
	if lastQueue > firstExchange {
		t.Fatal("el reset debe borrar todas las colas antes que cualquier exchange")
	}

	// Las colas de los flujos de US-08 y US-09 forman parte del reset.
	for _, work := range []string{
		"ia.alert-evidence-uploaded",
		"alert.ia-evidence-validated",
		"identity.alert-rejected",
		"identity.alert-reported",
		"alert.user-status-changed",
		"notification.alert-rejected",
		"notification.alert-expired",
		"notification.alert-seen",
	} {
		for _, suffix := range []string{".q", ".retry", ".dlq"} {
			if !deleted["queue:"+work+suffix] {
				t.Errorf("el reset no borró la cola %s", work+suffix)
			}
		}
	}
	for _, name := range []string{"vvi.unrouted.q"} {
		if !deleted["queue:"+name] {
			t.Errorf("el reset no borró la cola %s", name)
		}
	}
}
