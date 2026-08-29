package eventlog

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/vinculo-vial/core-service/internal/domain/event"
)

func TestHandler_LogsEventTypeAndActor(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	h := NewHandler(logger)

	envelope := event.Envelope{
		EventID:   "e1",
		EventType: "alert.speeding.detected",
		Timestamp: time.Now(),
		Producer:  "telemetry-service",
		Actor:     event.Actor{IssuerID: "device-42"},
		Payload:   json.RawMessage(`{}`),
	}

	if err := h.Handle(context.Background(), envelope); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "alert.speeding.detected") {
		t.Errorf("log output missing eventType: %s", out)
	}
	if !strings.Contains(out, "device-42") {
		t.Errorf("log output missing actor issuer_id: %s", out)
	}
}
