package platform

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestConsumerRejectsUnknownSchemaAndMismatchedAggregate(t *testing.T) {
	for _, e := range []Envelope{{SchemaVersion: 2, EventID: "019a0000-0000-7000-8000-000000000001", AggregateID: "019a0000-0000-7000-8000-000000000002", EventType: "identity.user_registered", OccurredAt: time.Now(), Payload: json.RawMessage(`{"user_id":"019a0000-0000-7000-8000-000000000002","email":"test@example.test"}`)}, {SchemaVersion: 1, EventID: "019a0000-0000-7000-8000-000000000001", AggregateID: "019a0000-0000-7000-8000-000000000002", EventType: "identity.user_registered", OccurredAt: time.Now(), Payload: json.RawMessage(`{"user_id":"019a0000-0000-7000-8000-000000000003","email":"test@example.test"}`)}} {
		if !errors.Is(Validate(e), ErrPoison) {
			t.Fatal("invalid source event accepted")
		}
	}
}
