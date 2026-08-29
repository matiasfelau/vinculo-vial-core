package event

import (
	"testing"
	"time"
)

func TestParse_Valid(t *testing.T) {
	body := []byte(`{
		"eventId": "11111111-1111-1111-1111-111111111111",
		"eventType": "alert.speeding.detected",
		"timestamp": "2026-08-29T12:00:00Z",
		"producer": "telemetry-service",
		"actor": {"issuer_id": "device-42", "issuer_type": "obu"},
		"payload": {"speedKmh": 130}
	}`)

	env, err := Parse(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if env.EventType != "alert.speeding.detected" {
		t.Errorf("EventType = %q, want alert.speeding.detected", env.EventType)
	}
	if env.Actor.IssuerID != "device-42" {
		t.Errorf("Actor.IssuerID = %q, want device-42", env.Actor.IssuerID)
	}

	wantTime := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	if !env.Timestamp.Equal(wantTime) {
		t.Errorf("Timestamp = %v, want %v", env.Timestamp, wantTime)
	}
}

func TestParse_RejectsUnknownTopLevelField(t *testing.T) {
	body := []byte(`{
		"eventId": "id",
		"eventType": "alert.x",
		"timestamp": "2026-08-29T12:00:00Z",
		"producer": "svc",
		"actor": {"issuer_id": "d1"},
		"payload": {},
		"unexpected": "field"
	}`)

	if _, err := Parse(body); err == nil {
		t.Fatal("expected error for unknown top-level field, got nil")
	}
}

func TestParse_MissingRequiredFields(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"missing eventId", `{"eventType":"alert.x","timestamp":"2026-08-29T12:00:00Z","producer":"svc","actor":{"issuer_id":"d1"},"payload":{}}`},
		{"missing eventType", `{"eventId":"id","timestamp":"2026-08-29T12:00:00Z","producer":"svc","actor":{"issuer_id":"d1"},"payload":{}}`},
		{"missing timestamp", `{"eventId":"id","eventType":"alert.x","producer":"svc","actor":{"issuer_id":"d1"},"payload":{}}`},
		{"missing producer", `{"eventId":"id","eventType":"alert.x","timestamp":"2026-08-29T12:00:00Z","actor":{"issuer_id":"d1"},"payload":{}}`},
		{"missing actor.issuer_id", `{"eventId":"id","eventType":"alert.x","timestamp":"2026-08-29T12:00:00Z","producer":"svc","actor":{},"payload":{}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Parse([]byte(tt.body)); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
}

func TestParse_MalformedJSON(t *testing.T) {
	if _, err := Parse([]byte(`not json`)); err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}
