// SPDX-License-Identifier: MIT
package audit

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"platform-contracts/gen/go/eventsv2"
	"testing"
	"time"
)

func TestAdminActionProjectionRetainsTypedNonUserTarget(t *testing.T) {
	id := uuid.Must(uuid.NewV7()).String()
	actor := uuid.NewString()
	session := uuid.NewString()
	payload, err := json.Marshal(eventsv2.AdminActionRecorded{AuditID: id, ActorUserID: actor, ActorSessionID: session, Operation: "role.permissions", TargetType: "role", TargetID: "support", Reason: "reduce access"})
	if err != nil {
		t.Fatal(err)
	}
	event := eventsv2.Envelope{SchemaVersion: 2, EventID: id, EventType: "identity.admin_action_recorded", Producer: "identity", AggregateType: "identity_admin_action", AggregateID: id, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: payload}
	if err := event.Validate(); err != nil {
		t.Fatal(err)
	}
	projected, err := projectionMetadata(event)
	if err != nil {
		t.Fatal(err)
	}
	if projected.UserID != nil || projected.ActorID == nil || *projected.ActorID != actor || projected.Operation == nil || *projected.Operation != "role.permissions" || projected.TargetType == nil || *projected.TargetType != "role" || projected.TargetID == nil || *projected.TargetID != "support" || projected.Reason == nil || *projected.Reason != "reduce access" {
		t.Fatalf("semantic metadata lost: %+v", projected)
	}
}

func TestAdminActionProjectionPersistsSafeFieldsAndDeduplicates(t *testing.T) {
	db := auditDatabase(t)
	ctx := context.Background()
	repo := &PostgresAudit{DB: db}
	id := uuid.Must(uuid.NewV7()).String()
	actor := uuid.NewString()
	session := uuid.NewString()
	payload, err := json.Marshal(eventsv2.AdminActionRecorded{AuditID: id, ActorUserID: actor, ActorSessionID: session, Operation: "oauth.client.rotate", TargetType: "oauth_client", TargetID: "website-client", Reason: "scheduled rotation"})
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{Audit: repo}
	event := eventsv2.Envelope{SchemaVersion: 2, EventID: id, EventType: "identity.admin_action_recorded", Producer: "identity", AggregateType: "identity_admin_action", AggregateID: id, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: payload}
	for range 2 {
		if err := s.ApplyV2(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	items, err := repo.List(ctx, Page{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].TargetID == nil || *items[0].TargetID != "website-client" || items[0].Reason == nil || *items[0].Reason != "scheduled rotation" || items[0].UserID != nil {
		t.Fatalf("stored action semantics lost: %+v", items)
	}
}
