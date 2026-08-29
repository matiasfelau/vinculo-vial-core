package verify

import (
	"errors"
	"testing"

	amqplib "github.com/rabbitmq/amqp091-go"

	"github.com/vinculo-vial/core-service/internal/topology"
)

type fakeChannel struct {
	exchangeErr map[string]error
	queueErr    map[string]error
}

func (f *fakeChannel) ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqplib.Table) error {
	return f.exchangeErr[name]
}

func (f *fakeChannel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args amqplib.Table) (amqplib.Queue, error) {
	return amqplib.Queue{Name: name}, f.queueErr[name]
}

func sampleTopology() topology.Topology {
	return topology.Topology{
		Exchanges: []topology.Exchange{
			{Name: "vvi.topic.exchange", Type: topology.ExchangeTopic, Durable: true},
		},
		Queues: []topology.Queue{
			{Name: "alert.created.q", Kind: topology.QueueKindWork, Durable: true},
			{Name: "alert.created.dlq", Kind: topology.QueueKindDLQ, Durable: true},
		},
	}
}

func TestVerify_Exitoso(t *testing.T) {
	fake := &fakeChannel{}
	v := New(fake)

	if err := v.Verify(sampleTopology()); err != nil {
		t.Fatalf("error inesperado: %v", err)
	}
}

func TestVerify_ColaFaltante(t *testing.T) {
	notFound := &amqplib.Error{Code: amqplib.NotFound, Reason: "NOT_FOUND", Server: true}
	fake := &fakeChannel{
		queueErr: map[string]error{"alert.created.dlq": notFound},
	}
	v := New(fake)

	err := v.Verify(sampleTopology())
	if err == nil {
		t.Fatal("se esperaba error por cola faltante")
	}
	if !errors.Is(err, notFound) {
		t.Fatalf("el error no envuelve la causa original: %v", err)
	}
}

func TestVerify_ExchangeFaltante(t *testing.T) {
	notFound := &amqplib.Error{Code: amqplib.NotFound, Reason: "NOT_FOUND", Server: true}
	fake := &fakeChannel{
		exchangeErr: map[string]error{"vvi.topic.exchange": notFound},
	}
	v := New(fake)

	if err := v.Verify(sampleTopology()); err == nil {
		t.Fatal("se esperaba error por exchange faltante")
	}
}
