//go:build smoke

package smoke

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	amqplib "github.com/rabbitmq/amqp091-go"

	vviamqp "github.com/vinculo-vial/core-service/internal/amqp"
	"github.com/vinculo-vial/core-service/internal/reset"
	"github.com/vinculo-vial/core-service/internal/topology"
	"github.com/vinculo-vial/core-service/pkg/config"
)

const (
	// probeRetryTTLMs es el TTL de las colas .retry de prueba: corto para que
	// el reintento llegue dentro del tiempo de la prueba. Las colas reales
	// conservan RETRY_TTL_MS, que es inmutable en el broker.
	probeRetryTTLMs = 1000

	probeA = "smoke-probe-a.alert-rejected"
	probeB = "smoke-probe-b.alert-rejected"

	userDeletedProbeA = "smoke-probe-a.user-deleted"
	userDeletedProbeB = "smoke-probe-b.user-deleted"

	// quietPeriod es cuánto se espera para dar por confirmado que una cola
	// NO recibe un mensaje.
	quietPeriod = 1500 * time.Millisecond

	publishTimeout = 2 * time.Second
)

// broker agrupa lo que comparten las pruebas de enrutamiento: la conexión
// con el broker real y la configuración con la que se declaró la topología.
type broker struct {
	conn   *amqplib.Connection
	cfg    config.Config
	logger *slog.Logger
}

// connectAndDeclare conecta con el broker y declara la topología vigente de
// forma idempotente, igual que cmd/core, de modo que la prueba no dependa de
// que el job haya corrido antes.
func connectAndDeclare(t *testing.T) broker {
	t.Helper()

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("cargando configuración: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	ctx, cancel := context.WithTimeout(context.Background(), brokerWaitTimeout+brokerRetryInterval)
	defer cancel()

	conn, err := vviamqp.WaitAndDial(ctx, func() (*amqplib.Connection, error) {
		return amqplib.Dial(cfg.RabbitMQURL)
	}, brokerWaitTimeout, brokerRetryInterval, logger)
	if err != nil {
		t.Fatalf("conectando con rabbitmq: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Logf("cerrando la conexión: %v", err)
		}
	})

	b := broker{conn: conn, cfg: cfg, logger: logger}
	b.apply(t, topology.Platform(cfg))
	return b
}

func (b broker) channel(t *testing.T) *amqplib.Channel {
	t.Helper()

	ch, err := b.conn.Channel()
	if err != nil {
		t.Fatalf("abriendo canal: %v", err)
	}
	t.Cleanup(func() {
		if err := ch.Close(); err != nil {
			t.Logf("cerrando el canal: %v", err)
		}
	})
	return ch
}

func (b broker) apply(t *testing.T, topo topology.Topology) {
	t.Helper()

	if err := vviamqp.NewDeclarer(b.channel(t), b.logger).Apply(topo); err != nil {
		t.Fatalf("declarando topología: %v", err)
	}
}

func (b broker) remove(t *testing.T, topo topology.Topology) {
	t.Helper()

	if err := reset.New(b.channel(t), b.logger).Apply(topo); err != nil {
		t.Fatalf("borrando topología de prueba: %v", err)
	}
}

// declareProbes declara un flujo de consumidor de prueba por cada base dada
// (W, W.retry y W.dlq cada uno, construidos por la misma función de flujo que
// usa Platform) bindeados al mismo evento routingKey, y los borra al
// terminar. Borra antes cualquier resto de una corrida abortada.
func (b broker) declareProbes(t *testing.T, routingKey string, bases ...string) {
	t.Helper()

	probeCfg := b.cfg
	probeCfg.RetryTTLMs = probeRetryTTLMs

	probes := make([]topology.Topology, 0, len(bases))
	for _, base := range bases {
		probes = append(probes, topology.ConsumerFlow(base, routingKey, probeCfg))
	}

	for _, p := range probes {
		b.remove(t, p)
	}
	t.Cleanup(func() {
		for _, p := range probes {
			cleanup := reset.New(b.channel(t), b.logger)
			if err := cleanup.Apply(p); err != nil {
				t.Errorf("limpiando colas de prueba: %v", err)
			}
		}
	})
	for _, p := range probes {
		b.apply(t, p)
	}
}

// requireNoConsumers falla si la cola tiene consumidores activos: un
// consumidor real competiría por los mensajes de la prueba.
func (b broker) requireNoConsumers(t *testing.T, queue string) {
	t.Helper()

	q, err := b.channel(t).QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("verificando la cola %q: %v", queue, err)
	}
	if q.Consumers > 0 {
		t.Fatalf("la cola %q tiene %d consumidor(es) activo(s): detener el servicio que la consume antes de correr la prueba de humo", queue, q.Consumers)
	}
}

func (b broker) depth(t *testing.T, queue string) int {
	t.Helper()

	q, err := b.channel(t).QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consultando la cola %q: %v", queue, err)
	}
	return q.Messages
}

func (b broker) consume(t *testing.T, queue string) <-chan amqplib.Delivery {
	t.Helper()

	deliveries, err := b.channel(t).Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("suscribiendo a %q: %v", queue, err)
	}
	return deliveries
}

func (b broker) publish(t *testing.T, exchange, routingKey, body string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()

	err := b.channel(t).PublishWithContext(ctx, exchange, routingKey, false, false, amqplib.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqplib.Persistent,
		Body:         []byte(body),
	})
	if err != nil {
		t.Fatalf("publicando en %q con routing key %q: %v", exchange, routingKey, err)
	}
}

func newMarker(tag string) string {
	return fmt.Sprintf("smoke-%d-%s", time.Now().UnixNano(), tag)
}

// mustReceive espera una entrega en la cola y exige que lleve el marcador.
func mustReceive(t *testing.T, deliveries <-chan amqplib.Delivery, queue, marker string, within time.Duration) amqplib.Delivery {
	t.Helper()

	select {
	case msg, open := <-deliveries:
		if !open {
			t.Fatalf("el canal de consumo de %q se cerró antes de recibir el mensaje", queue)
		}
		if string(msg.Body) != marker {
			t.Fatalf("llegó a %q un mensaje de otra corrida (%q), se esperaba %q", queue, msg.Body, marker)
		}
		return msg
	case <-time.After(within):
		t.Fatalf("no llegó ningún mensaje a %q dentro de %s", queue, within)
	}
	return amqplib.Delivery{}
}

// mustNotReceive exige que la cola no reciba nada durante quietPeriod.
func mustNotReceive(t *testing.T, deliveries <-chan amqplib.Delivery, queue string) {
	t.Helper()

	select {
	case msg := <-deliveries:
		t.Fatalf("la cola %q recibió un mensaje que no le correspondía: %q", queue, msg.Body)
	case <-time.After(quietPeriod):
	}
}

func mustAck(t *testing.T, msg amqplib.Delivery, queue string) {
	t.Helper()

	if err := msg.Ack(false); err != nil {
		t.Fatalf("confirmando el mensaje de %q: %v", queue, err)
	}
}

// deathCount devuelve el contador de la entrada de x-death de la cola y el
// motivo dados, o 0 si el mensaje no pasó por ahí.
func deathCount(headers amqplib.Table, queue, reason string) int64 {
	entries, _ := headers["x-death"].([]interface{})
	for _, raw := range entries {
		entry, ok := raw.(amqplib.Table)
		if !ok {
			continue
		}
		if entry["queue"] == queue && entry["reason"] == reason {
			count, _ := entry["count"].(int64)
			return count
		}
	}
	return 0
}

// TestRetry_ReentregaSoloALaColaQueHizoNack publica alert.rejected con tres
// colas bindeadas (la real de Identity y dos de prueba). Un nack sin requeue
// en una de las de prueba debe volver únicamente a esa cola, pasando por su
// .retry: las demás, que ya hicieron ack, no reciben una segunda copia. Con
// el ruteo anterior (retry hacia vvi.topic.exchange por la routing key del
// evento) la reentrega llegaría a todas.
func TestRetry_ReentregaSoloALaColaQueHizoNack(t *testing.T) {
	b := connectAndDeclare(t)

	identityQueue, ok := workQueueFor(topology.Platform(b.cfg), b.cfg.MainExchange, topology.AlertRejectedRoutingKey)
	if !ok {
		t.Fatalf("la topología no liga ninguna cola a %q en %q", topology.AlertRejectedRoutingKey, b.cfg.MainExchange)
	}
	b.requireNoConsumers(t, identityQueue)
	b.declareProbes(t, topology.AlertRejectedRoutingKey, probeA, probeB)

	probeAWork, probeBWork := probeA+".q", probeB+".q"
	deliveriesA := b.consume(t, probeAWork)
	deliveriesB := b.consume(t, probeBWork)
	deliveriesIdentity := b.consume(t, identityQueue)

	marker := newMarker("retry")
	b.publish(t, b.cfg.MainExchange, topology.AlertRejectedRoutingKey, marker)

	msgA := mustReceive(t, deliveriesA, probeAWork, marker, deliveryTimeout)
	msgB := mustReceive(t, deliveriesB, probeBWork, marker, deliveryTimeout)
	msgIdentity := mustReceive(t, deliveriesIdentity, identityQueue, marker, deliveryTimeout)

	mustAck(t, msgB, probeBWork)
	mustAck(t, msgIdentity, identityQueue)
	if err := msgA.Nack(false, false); err != nil {
		t.Fatalf("nack del mensaje de %q: %v", probeAWork, err)
	}

	retried := mustReceive(t, deliveriesA, probeAWork, marker, probeRetryTTLMs*time.Millisecond+deliveryTimeout)

	if n := deathCount(retried.Headers, probeAWork, "rejected"); n != 1 {
		t.Errorf("x-death registra %d rechazos en %q, se esperaba 1", n, probeAWork)
	}
	if n := deathCount(retried.Headers, probeA+".retry", "expired"); n != 1 {
		t.Errorf("x-death registra %d vencimientos en %q, se esperaba 1: el mensaje no pasó por el retry", n, probeA+".retry")
	}

	mustNotReceive(t, deliveriesB, probeBWork)
	mustNotReceive(t, deliveriesIdentity, identityQueue)
	for _, queue := range []string{probeBWork, identityQueue} {
		if n := b.depth(t, queue); n != 0 {
			t.Errorf("la cola %q tiene %d mensaje(s) pendiente(s): el reintento no debía llegarle", queue, n)
		}
	}

	mustAck(t, retried, probeAWork)
}

// TestDLQ_RutaPorNombreDeColaDeTrabajo publica en vvi.dlq.exchange con la
// routing key W, como hace un consumidor al agotar MAX_RETRIES: el mensaje
// debe quedar solo en W.dlq, no en la DLQ de otro consumidor del mismo evento
// ni en la cola de trabajo.
func TestDLQ_RutaPorNombreDeColaDeTrabajo(t *testing.T) {
	b := connectAndDeclare(t)
	b.declareProbes(t, topology.AlertRejectedRoutingKey, probeA, probeB)

	probeAWork := probeA + ".q"
	deliveriesA := b.consume(t, probeA+".dlq")
	deliveriesB := b.consume(t, probeB+".dlq")

	marker := newMarker("dlq")
	b.publish(t, b.cfg.DLQExchange, probeAWork, marker)

	msg := mustReceive(t, deliveriesA, probeA+".dlq", marker, deliveryTimeout)
	mustNotReceive(t, deliveriesB, probeB+".dlq")
	mustAck(t, msg, probeA+".dlq")

	for _, queue := range []string{probeAWork, probeB + ".q"} {
		if n := b.depth(t, queue); n != 0 {
			t.Errorf("la cola de trabajo %q tiene %d mensaje(s): la DLQ no debe reentregar", queue, n)
		}
	}
}

// TestRouting_ColaDeIARecibeGreenYellowYRed publica las tres routing keys
// del veredicto de IA y confirma que alert.ia-evidence-validated.q, bindeada
// con el wildcard ia.evidence.validated.*, recibe cada una.
func TestRouting_ColaDeIARecibeGreenYellowYRed(t *testing.T) {
	b := connectAndDeclare(t)

	queue, ok := workQueueFor(topology.Platform(b.cfg), b.cfg.MainExchange, topology.IAEvidenceValidatedBindingKey)
	if !ok {
		t.Fatalf("la topología no liga ninguna cola a %q en %q", topology.IAEvidenceValidatedBindingKey, b.cfg.MainExchange)
	}
	b.requireNoConsumers(t, queue)

	deliveries := b.consume(t, queue)

	for _, verdict := range []string{"green", "yellow", "red"} {
		routingKey := "ia.evidence.validated." + verdict

		t.Run(routingKey, func(t *testing.T) {
			marker := newMarker(verdict)
			b.publish(t, b.cfg.MainExchange, routingKey, marker)

			mustAck(t, mustReceive(t, deliveries, queue, marker, deliveryTimeout), queue)
		})
	}
}

const (
	alertUserDeletedQueue   = "alert.user-deleted.q"
	vehicleUserDeletedQueue = "vehicle.user-deleted.q"
)

// TestRouting_UserDeletedLlegaALasDosColas publica user.deleted en el
// exchange principal y confirma que lo reciben las colas de trabajo de Alert
// y de Vehicle, cada una con su propia copia.
func TestRouting_UserDeletedLlegaALasDosColas(t *testing.T) {
	b := connectAndDeclare(t)

	queues := []string{alertUserDeletedQueue, vehicleUserDeletedQueue}
	deliveries := map[string]<-chan amqplib.Delivery{}
	for _, queue := range queues {
		b.requireNoConsumers(t, queue)
		deliveries[queue] = b.consume(t, queue)
	}

	marker := newMarker("user-deleted")
	b.publish(t, b.cfg.MainExchange, topology.UserDeletedRoutingKey, marker)

	for _, queue := range queues {
		mustAck(t, mustReceive(t, deliveries[queue], queue, marker, deliveryTimeout), queue)
	}
}

// TestRetry_UserDeletedReentregaSoloALaColaQueFallo publica user.deleted con
// cuatro colas bindeadas (las reales de Alert y Vehicle y dos de prueba). Un
// nack sin requeue en una de las de prueba debe volver únicamente a esa cola,
// pasando por su .retry: las demás, que ya hicieron ack, no reciben una
// segunda copia.
func TestRetry_UserDeletedReentregaSoloALaColaQueFallo(t *testing.T) {
	b := connectAndDeclare(t)

	b.requireNoConsumers(t, alertUserDeletedQueue)
	b.requireNoConsumers(t, vehicleUserDeletedQueue)
	b.declareProbes(t, topology.UserDeletedRoutingKey, userDeletedProbeA, userDeletedProbeB)

	probeAWork, probeBWork := userDeletedProbeA+".q", userDeletedProbeB+".q"
	deliveriesA := b.consume(t, probeAWork)
	deliveriesB := b.consume(t, probeBWork)
	deliveriesAlert := b.consume(t, alertUserDeletedQueue)
	deliveriesVehicle := b.consume(t, vehicleUserDeletedQueue)

	marker := newMarker("user-deleted-retry")
	b.publish(t, b.cfg.MainExchange, topology.UserDeletedRoutingKey, marker)

	msgA := mustReceive(t, deliveriesA, probeAWork, marker, deliveryTimeout)
	msgB := mustReceive(t, deliveriesB, probeBWork, marker, deliveryTimeout)
	msgAlert := mustReceive(t, deliveriesAlert, alertUserDeletedQueue, marker, deliveryTimeout)
	msgVehicle := mustReceive(t, deliveriesVehicle, vehicleUserDeletedQueue, marker, deliveryTimeout)

	mustAck(t, msgB, probeBWork)
	mustAck(t, msgAlert, alertUserDeletedQueue)
	mustAck(t, msgVehicle, vehicleUserDeletedQueue)
	if err := msgA.Nack(false, false); err != nil {
		t.Fatalf("nack del mensaje de %q: %v", probeAWork, err)
	}

	retried := mustReceive(t, deliveriesA, probeAWork, marker, probeRetryTTLMs*time.Millisecond+deliveryTimeout)

	if n := deathCount(retried.Headers, probeAWork, "rejected"); n != 1 {
		t.Errorf("x-death registra %d rechazos en %q, se esperaba 1", n, probeAWork)
	}
	if n := deathCount(retried.Headers, userDeletedProbeA+".retry", "expired"); n != 1 {
		t.Errorf("x-death registra %d vencimientos en %q, se esperaba 1: el mensaje no pasó por el retry", n, userDeletedProbeA+".retry")
	}

	mustNotReceive(t, deliveriesB, probeBWork)
	mustNotReceive(t, deliveriesAlert, alertUserDeletedQueue)
	mustNotReceive(t, deliveriesVehicle, vehicleUserDeletedQueue)
	for _, queue := range []string{probeBWork, alertUserDeletedQueue, vehicleUserDeletedQueue} {
		if n := b.depth(t, queue); n != 0 {
			t.Errorf("la cola %q tiene %d mensaje(s) pendiente(s): el reintento no debía llegarle", queue, n)
		}
	}

	mustAck(t, retried, probeAWork)
}

// auditQueues asocia cada routing key de Audit con su única cola de trabajo.
var auditQueues = map[string]string{
	topology.AuditAlertRecordedRoutingKey:  "audit.alert-recorded.q",
	topology.AuditAlertDeliveredRoutingKey: "audit.alert-delivered.q",
	topology.AuditAlertDiscardedRoutingKey: "audit.alert-discarded.q",
}

// TestRouting_AuditAlertLlegaSoloASuCola publica cada routing key audit.alert.*
// en el exchange principal y confirma que la recibe únicamente su cola: las
// otras dos de Audit no deben recibir copia.
func TestRouting_AuditAlertLlegaSoloASuCola(t *testing.T) {
	b := connectAndDeclare(t)

	deliveries := map[string]<-chan amqplib.Delivery{}
	for _, queue := range auditQueues {
		b.requireNoConsumers(t, queue)
		deliveries[queue] = b.consume(t, queue)
	}

	for routingKey, want := range auditQueues {
		t.Run(routingKey, func(t *testing.T) {
			marker := newMarker(routingKey)
			b.publish(t, b.cfg.MainExchange, routingKey, marker)

			mustAck(t, mustReceive(t, deliveries[want], want, marker, deliveryTimeout), want)

			for queue, ch := range deliveries {
				if queue != want {
					mustNotReceive(t, ch, queue)
				}
			}
		})
	}
}
