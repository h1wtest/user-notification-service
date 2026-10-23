// SPDX-License-Identifier: MIT
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"platform-contracts/gen/go/eventsv2"
	"user-notification-service/internal/platform"
)

type projectionStore struct {
	Repository
	event AuditEvent
	count int
}

func (p *projectionStore) ProjectMetadata(_ context.Context, e AuditEvent, _ string) error {
	p.event = e
	p.count++
	return nil
}

func TestReplayProjectionRetainsActorButNotReasonOrBody(t *testing.T) {
	p := &projectionStore{}
	s := &Service{Audit: p}
	raw, err := json.Marshal(eventsv2.DeliveryReplayed{DeliveryID: testTarget, UserID: testTarget, ActorID: testActor, Reason: "private incident narrative"})
	if err != nil {
		t.Fatal(err)
	}
	e := eventsv2.Envelope{SchemaVersion: 2, EventID: testCommand, EventType: "notification.delivery_replayed", Producer: "notification", AggregateType: "delivery", AggregateID: testTarget, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: raw}
	if err = s.ApplyV2(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if p.event.ActorID == nil || *p.event.ActorID != testActor || p.event.UserID == nil || *p.event.UserID != testTarget {
		t.Fatalf("lost safe attribution: %+v", p.event)
	}
	safe, _ := json.Marshal(p.event)
	if strings.Contains(string(safe), "private incident") || strings.Contains(string(safe), "payload") || p.event.Reason != nil {
		t.Fatalf("private replay payload escaped: %s", safe)
	}
}
func TestCatalogProjectionNeverLabelsRevisionAsUser(t *testing.T) {
	p := &projectionStore{}
	s := &Service{Audit: p}
	raw, err := json.Marshal(eventsv2.RevisionPublished{ProductID: testTarget, PlanID: testCommand, RevisionID: testCommand, ActorUserID: testActor, RevisionVersion: 1, PlanVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	e := eventsv2.Envelope{SchemaVersion: 2, EventID: testCommand, EventType: "catalog.revision_published", Producer: "catalog", AggregateType: "plan", AggregateID: testCommand, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: raw}
	if err = s.ApplyV2(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if p.event.UserID != nil || p.event.ActorID == nil || *p.event.ActorID != testActor || p.event.AggregateID == nil || *p.event.AggregateID != testCommand {
		t.Fatalf("wrong audit identities: %+v", p.event)
	}
}
func TestInvalidPublicProjectionPoisonsBeforePersistence(t *testing.T) {
	p := &projectionStore{}
	s := &Service{Audit: p}
	e := eventsv2.Envelope{SchemaVersion: 2, EventID: testCommand, EventType: "profile.updated", Producer: "profile", AggregateType: "user", AggregateID: testTarget, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: json.RawMessage(`{"user_id":"not-a-uuid"}`)}
	if err := s.ApplyV2(context.Background(), e); !errors.Is(err, platform.ErrPoison) || p.count != 0 {
		t.Fatalf("invalid event reached storage: %v count=%d", err, p.count)
	}
}

func TestTemplatePublicationProjectsActorWithoutTemplateBodies(t *testing.T) {
	p := &projectionStore{}
	s := &Service{Audit: p}
	raw, err := json.Marshal(eventsv2.TemplatePublished{EventType: "subscription.changed", Version: 2, ActorID: testActor})
	if err != nil {
		t.Fatal(err)
	}
	e := eventsv2.Envelope{SchemaVersion: 2, EventID: testCommand, EventType: "notification.template_published", Producer: "notification", AggregateType: "notification_template", AggregateID: testTarget, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: raw}
	if err = s.ApplyV2(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if p.event.ActorID == nil || *p.event.ActorID != testActor || p.event.UserID != nil {
		t.Fatalf("template audit attribution: %+v", p.event)
	}
}
