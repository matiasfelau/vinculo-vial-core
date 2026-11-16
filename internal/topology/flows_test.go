package topology

import (
	"reflect"
	"sort"
	"strings"
	"testing"
)

const (
	iaValidatedWork = "alert.ia-evidence-validated.q"
	iaValidatedBase = "alert.ia-evidence-validated"
)

// contractFlows replica la tabla "Topología a declarar" del contrato: cola de
// trabajo W → patrón de binding en el exchange principal.
func contractFlows() map[string]string {
	return map[string]string{
		"notification.alert-created.q":           "alert.created",
		"notification.sessions-revoked.q":        "user.sessions.revoked",
		"ia.alert-evidence-uploaded.q":           "alert.evidence.uploaded",
		iaValidatedWork:                          "ia.evidence.validated.*",
		"identity.alert-rejected.q":              "alert.rejected",
		"identity.alert-reported.q":              "alert.reported",
		"alert.user-status-changed.q":            "user.status.changed",
		"notification.alert-rejected.q":          "alert.rejected",
		"notification.alert-expired.q":           "alert.expired",
		"notification.alert-seen.q":              "alert.seen",
		"alert.vehicle-pending-claimed.q":        "vehicle.pending.claimed",
		"notification.alert-pending-delivered.q": "alert.pending.delivered",
		"alert.user-deleted.q":                   "user.deleted",
		"vehicle.user-deleted.q":                 "user.deleted",
		"audit.alert-recorded.q":                 "audit.alert.recorded",
		"audit.alert-delivered.q":                "audit.alert.delivered",
		"audit.alert-discarded.q":                "audit.alert.discarded",
		"notification.email-requested.q":         "email.requested",
	}
}

// topicMatches implementa el matching de un topic exchange de AMQP: las
// palabras se separan con ".", "*" reemplaza exactamente una palabra y "#"
// reemplaza cero o más.
func topicMatches(pattern, key string) bool {
	return matchWords(strings.Split(pattern, "."), strings.Split(key, "."))
}

func matchWords(pattern, key []string) bool {
	if len(pattern) == 0 {
		return len(key) == 0
	}
	switch pattern[0] {
	case "#":
		for i := 0; i <= len(key); i++ {
			if matchWords(pattern[1:], key[i:]) {
				return true
			}
		}
		return false
	case "*":
		return len(key) > 0 && matchWords(pattern[1:], key[1:])
	default:
		return len(key) > 0 && pattern[0] == key[0] && matchWords(pattern[1:], key[1:])
	}
}

// queuesMatching devuelve, ordenadas, las colas que recibirían un mensaje
// publicado en exchange con la routing key dada.
func queuesMatching(tp Topology, exchange, routingKey string) []string {
	var queues []string
	for _, b := range tp.Bindings {
		if b.Exchange == exchange && topicMatches(b.RoutingKey, routingKey) {
			queues = append(queues, b.Queue)
		}
	}
	sort.Strings(queues)
	return queues
}

func TestTopicMatches(t *testing.T) {
	cases := []struct {
		pattern, key string
		want         bool
	}{
		{"ia.evidence.validated.*", "ia.evidence.validated.green", true},
		{"ia.evidence.validated.*", "ia.evidence.validated", false},
		{"ia.evidence.validated.*", "ia.evidence.validated.green.extra", false},
		{"ia.evidence.validated.*", "ia.evidence.other.green", false},
		{"alert.#", "alert", true},
		{"alert.#", "alert.created.extra", true},
		{"alert.created", "alert.created", true},
		{"alert.created", "alert.rejected", false},
	}
	for _, c := range cases {
		if got := topicMatches(c.pattern, c.key); got != c.want {
			t.Errorf("topicMatches(%q, %q) = %v, se esperaba %v", c.pattern, c.key, got, c.want)
		}
	}
}

func TestPlatform_DeclaraExactamenteLosDieciochoFlujosDelContrato(t *testing.T) {
	tp := Platform(testConfig())

	if got := len(contractFlows()); got != 18 {
		t.Fatalf("el contrato enumera %d flujos, se esperaban 18", got)
	}

	want := []string{unroutedQueueName}
	for work := range contractFlows() {
		base := strings.TrimSuffix(work, ".q")
		want = append(want, work, base+".retry", base+".dlq")
	}
	sort.Strings(want)

	var got []string
	for _, q := range tp.Queues {
		got = append(got, q.Name)
		if !q.Durable {
			t.Errorf("la cola %q no es durable", q.Name)
		}
	}
	sort.Strings(got)

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("colas declaradas = %v, se esperaba %v", got, want)
	}
}

// TestPlatform_ArgumentosDeCadaFlujoDelContrato verifica, para los 18 flujos
// (incluidos los tres de Audit y el de email), los argumentos inmutables de la cola de
// trabajo, de su .retry y de su .dlq. Cambiarlos exige borrar y recrear la
// cola, así que cualquier desvío debe romper acá y no con un 406 en el broker.
func TestPlatform_ArgumentosDeCadaFlujoDelContrato(t *testing.T) {
	cfg := testConfig()
	tp := Platform(cfg)

	queues := map[string]Queue{}
	for _, q := range tp.Queues {
		queues[q.Name] = q
	}

	for _, newWork := range []string{
		"audit.alert-recorded.q", "audit.alert-delivered.q", "audit.alert-discarded.q",
		"notification.email-requested.q",
	} {
		if _, ok := contractFlows()[newWork]; !ok {
			t.Fatalf("el flujo %q no está en el contrato del test", newWork)
		}
	}

	for work := range contractFlows() {
		base := strings.TrimSuffix(work, ".q")
		w, retry, dlq := queues[work], queues[base+".retry"], queues[base+".dlq"]

		wantWork := map[string]any{
			"x-dead-letter-exchange":    cfg.RetryExchange,
			"x-dead-letter-routing-key": work,
		}
		if !reflect.DeepEqual(w.Arguments, wantWork) {
			t.Errorf("%s: argumentos = %v, se esperaba %v", work, w.Arguments, wantWork)
		}

		wantRetry := map[string]any{
			"x-message-ttl":             cfg.RetryTTLMs,
			"x-dead-letter-exchange":    DefaultExchange,
			"x-dead-letter-routing-key": work,
		}
		if !reflect.DeepEqual(retry.Arguments, wantRetry) {
			t.Errorf("%s: argumentos = %v, se esperaba %v", retry.Name, retry.Arguments, wantRetry)
		}

		if len(dlq.Arguments) != 0 {
			t.Errorf("%s: la DLQ no lleva argumentos, tiene %v", dlq.Name, dlq.Arguments)
		}
		for _, q := range []Queue{w, retry, dlq} {
			if !q.Durable {
				t.Errorf("la cola %q no es durable", q.Name)
			}
		}
	}
}

func TestPlatform_CadaColaDeTrabajoSeLigaAlExchangePrincipalSoloPorSuEvento(t *testing.T) {
	tp := Platform(testConfig())

	for work, eventKey := range contractFlows() {
		var keys []string
		for _, b := range tp.Bindings {
			if b.Queue == work && b.Exchange == "vvi.topic.exchange" {
				keys = append(keys, b.RoutingKey)
			}
		}
		if !reflect.DeepEqual(keys, []string{eventKey}) {
			t.Errorf("bindings de %q en vvi.topic.exchange = %v, se esperaba [%q]", work, keys, eventKey)
		}
	}
}

func TestPlatform_ColaDeIARecibeLasTresRoutingKeysDelVeredicto(t *testing.T) {
	tp := Platform(testConfig())

	for _, verdict := range []string{"green", "yellow", "red"} {
		key := "ia.evidence.validated." + verdict
		got := queuesMatching(tp, "vvi.topic.exchange", key)
		if !reflect.DeepEqual(got, []string{iaValidatedWork}) {
			t.Errorf("publicar %q en vvi.topic.exchange llega a %v, se esperaba solo [%q]", key, got, iaValidatedWork)
		}
	}

	for _, key := range []string{"ia.evidence.validated", "ia.evidence.validated.green.extra", "ia.evidence.uploaded.green"} {
		if got := queuesMatching(tp, "vvi.topic.exchange", key); len(got) != 0 {
			t.Errorf("publicar %q en vvi.topic.exchange no debería llegar a ninguna cola, llega a %v", key, got)
		}
	}
}

func TestPlatform_RetryYDLQDeIARutenPorNombreDeColaYNoPorElWildcard(t *testing.T) {
	cfg := testConfig()
	tp := Platform(cfg)

	var work, retry Queue
	for _, q := range tp.Queues {
		switch q.Name {
		case iaValidatedWork:
			work = q
		case iaValidatedBase + ".retry":
			retry = q
		}
	}
	if work.Name == "" || retry.Name == "" {
		t.Fatalf("faltan %q o %q en la topología", iaValidatedWork, iaValidatedBase+".retry")
	}

	if work.Arguments["x-dead-letter-exchange"] != cfg.RetryExchange || work.Arguments["x-dead-letter-routing-key"] != iaValidatedWork {
		t.Errorf("la cola de trabajo hace dead-letter a (%v, %v), se esperaba (%q, %q)",
			work.Arguments["x-dead-letter-exchange"], work.Arguments["x-dead-letter-routing-key"], cfg.RetryExchange, iaValidatedWork)
	}
	if retry.Arguments["x-dead-letter-exchange"] != DefaultExchange || retry.Arguments["x-dead-letter-routing-key"] != iaValidatedWork {
		t.Errorf("retry hace dead-letter a (%v, %v), se esperaba (%q, %q)",
			retry.Arguments["x-dead-letter-exchange"], retry.Arguments["x-dead-letter-routing-key"], DefaultExchange, iaValidatedWork)
	}

	wantBindings := map[string]string{
		iaValidatedBase + ".retry": cfg.RetryExchange,
		iaValidatedBase + ".dlq":   cfg.DLQExchange,
	}
	found := map[string]bool{}
	for _, b := range tp.Bindings {
		exchange, ok := wantBindings[b.Queue]
		if !ok {
			continue
		}
		found[b.Queue] = true
		if b.Exchange != exchange || b.RoutingKey != iaValidatedWork {
			t.Errorf("binding de %q = (%q, %q), se esperaba (%q, %q)", b.Queue, b.Exchange, b.RoutingKey, exchange, iaValidatedWork)
		}
		if strings.ContainsAny(b.RoutingKey, "*#") {
			t.Errorf("binding de %q usa el wildcard %q", b.Queue, b.RoutingKey)
		}
	}
	for queue := range wantBindings {
		if !found[queue] {
			t.Errorf("no se encontró binding para %q", queue)
		}
	}

	for _, verdict := range []string{"green", "yellow", "red"} {
		key := "ia.evidence.validated." + verdict
		if got := queuesMatching(tp, cfg.RetryExchange, key); len(got) != 0 {
			t.Errorf("publicar %q en %s llegaría a %v: el retry no debe rutear por la routing key del evento", key, cfg.RetryExchange, got)
		}
		if got := queuesMatching(tp, cfg.DLQExchange, key); len(got) != 0 {
			t.Errorf("publicar %q en %s llegaría a %v: la DLQ no debe rutear por la routing key del evento", key, cfg.DLQExchange, got)
		}
	}
}

func TestPlatform_RetryYDLQDeCadaFlujoRecibenSoloSuPropioMensaje(t *testing.T) {
	cfg := testConfig()
	tp := Platform(cfg)

	for work := range contractFlows() {
		base := strings.TrimSuffix(work, ".q")

		if got := queuesMatching(tp, cfg.RetryExchange, work); !reflect.DeepEqual(got, []string{base + ".retry"}) {
			t.Errorf("publicar %q en %s llega a %v, se esperaba solo [%q]", work, cfg.RetryExchange, got, base+".retry")
		}
		if got := queuesMatching(tp, cfg.DLQExchange, work); !reflect.DeepEqual(got, []string{base + ".dlq"}) {
			t.Errorf("publicar %q en %s llega a %v, se esperaba solo [%q]", work, cfg.DLQExchange, got, base+".dlq")
		}
	}
}

func TestPlatform_CadaEventoDelContratoLlegaSoloASusConsumidores(t *testing.T) {
	tp := Platform(testConfig())

	// alert.rejected tiene dos consumidores contratados: Identity (penaliza) y
	// Notification (avisa al emisor), cada uno con su propia cola de trabajo.
	// user.deleted también tiene dos: Alert y Vehicle. Cada evento audit.alert.*
	// tiene un único consumidor: Audit. email.requested también tiene uno solo:
	// Notification.
	concreteKeys := map[string][]string{
		"alert.created":                {"notification.alert-created.q"},
		"user.sessions.revoked":        {"notification.sessions-revoked.q"},
		"alert.evidence.uploaded":      {"ia.alert-evidence-uploaded.q"},
		"ia.evidence.validated.green":  {iaValidatedWork},
		"ia.evidence.validated.yellow": {iaValidatedWork},
		"ia.evidence.validated.red":    {iaValidatedWork},
		"alert.rejected":               {"identity.alert-rejected.q", "notification.alert-rejected.q"},
		"alert.reported":               {"identity.alert-reported.q"},
		"user.status.changed":          {"alert.user-status-changed.q"},
		"alert.expired":                {"notification.alert-expired.q"},
		"alert.seen":                   {"notification.alert-seen.q"},
		"vehicle.pending.claimed":      {"alert.vehicle-pending-claimed.q"},
		"alert.pending.delivered":      {"notification.alert-pending-delivered.q"},
		"user.deleted":                 {"alert.user-deleted.q", "vehicle.user-deleted.q"},
		"audit.alert.recorded":         {"audit.alert-recorded.q"},
		"audit.alert.delivered":        {"audit.alert-delivered.q"},
		"audit.alert.discarded":        {"audit.alert-discarded.q"},
		"email.requested":              {"notification.email-requested.q"},
	}

	for key, want := range concreteKeys {
		if got := queuesMatching(tp, "vvi.topic.exchange", key); !reflect.DeepEqual(got, want) {
			t.Errorf("publicar %q en vvi.topic.exchange llega a %v, se esperaba %v", key, got, want)
		}
	}
}
