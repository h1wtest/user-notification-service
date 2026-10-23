// SPDX-License-Identifier: MIT
package audit

import (
	"encoding/json"
	"github.com/google/uuid"
	"platform-contracts/gen/go/eventsv2"
	"testing"
	"time"
)

func TestPaymentReviewProjectionPreservesActorAndSafeDecisionMetadata(t *testing.T) {
	actor, user, order, evidence := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	id := uuid.Must(uuid.NewV7()).String()
	raw, err := json.Marshal(eventsv2.PaymentReviewed{DecisionID: id, OrderID: order, UserID: user, ActorUserID: actor, Operation: "accept", Reason: "evidence reconciled", EvidenceID: &evidence, Version: 7})
	if err != nil {
		t.Fatal(err)
	}
	e := eventsv2.Envelope{SchemaVersion: 2, EventID: id, EventType: "billing.payment_reviewed", Producer: "billing", AggregateType: "billing_review", AggregateID: id, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: raw}
	if err = e.Validate(); err != nil {
		t.Fatal(err)
	}
	row, err := projectionMetadata(e)
	if err != nil {
		t.Fatal(err)
	}
	if row.ActorID == nil || *row.ActorID != actor || row.UserID == nil || *row.UserID != user || row.Operation == nil || *row.Operation != "accept" || row.Reason == nil || *row.Reason != "evidence reconciled" {
		t.Fatalf("decision metadata lost: %+v", row)
	}
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err = json.Unmarshal(encoded, &metadata); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"provider_payload", "raw_evidence", "payload", "email", "address"} {
		if _, ok := metadata[forbidden]; ok {
			t.Fatalf("unsafe field %q projected", forbidden)
		}
	}
}
