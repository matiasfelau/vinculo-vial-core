// Package event contains the domain model for the platform-standard Event
// Envelope exchanged over the Vínculo Vial message broker.
package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Envelope is the standard message contract every consumer on the platform
// must honor: {eventId, eventType, timestamp, producer, actor, payload}.
type Envelope struct {
	EventID   string          `json:"eventId"`
	EventType string          `json:"eventType"`
	Timestamp time.Time       `json:"timestamp"`
	Producer  string          `json:"producer"`
	Actor     Actor           `json:"actor"`
	Payload   json.RawMessage `json:"payload"`
}

// Actor identifies who or what issued the event.
type Actor struct {
	IssuerID string
}

// UnmarshalJSON extracts issuer_id from the actor object. It intentionally
// tolerates additional actor fields the platform may add later, since only
// issuer_id is part of this service's contract.
func (a *Actor) UnmarshalJSON(data []byte) error {
	var raw struct {
		IssuerID string `json:"issuer_id"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	a.IssuerID = raw.IssuerID
	return nil
}

// Parse strictly decodes an Event Envelope: the top-level shape must match
// the platform contract exactly (no unknown fields) and all required fields
// must be present.
func Parse(body []byte) (Envelope, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()

	var env Envelope
	if err := dec.Decode(&env); err != nil {
		return Envelope{}, fmt.Errorf("decode event envelope: %w", err)
	}

	if err := env.Validate(); err != nil {
		return Envelope{}, err
	}

	return env, nil
}

// Validate reports whether all fields required by the envelope contract are
// present.
func (e Envelope) Validate() error {
	switch {
	case e.EventID == "":
		return errors.New("event envelope: missing eventId")
	case e.EventType == "":
		return errors.New("event envelope: missing eventType")
	case e.Timestamp.IsZero():
		return errors.New("event envelope: missing timestamp")
	case e.Producer == "":
		return errors.New("event envelope: missing producer")
	case e.Actor.IssuerID == "":
		return errors.New("event envelope: missing actor.issuer_id")
	}
	return nil
}
