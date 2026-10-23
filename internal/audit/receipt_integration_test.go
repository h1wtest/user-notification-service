// SPDX-License-Identifier: MIT
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"platform-contracts/gen/go/eventsv2"
	"user-notification-service/internal/platform"
)

func TestIntegrationV2IdentityCollisionCannotAcknowledgeChangedEvent(t *testing.T) {
	db := auditDatabase(t)
	s := &Service{Audit: &PostgresAudit{DB: db}}
	ctx := context.Background()
	user := uuid.NewString()
	payload, err := json.Marshal(eventsv2.ProfileUpdated{UserID: user, WebsiteLocale: "en", NotificationLocale: "en", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	original := eventsv2.Envelope{SchemaVersion: 2, EventID: id.String(), EventType: "profile.updated", Producer: "profile", AggregateType: "user", AggregateID: user, AggregateVersion: 1, OccurredAt: time.Now().UTC(), Payload: payload}
	if err = s.ApplyV2(ctx, original); err != nil {
		t.Fatal(err)
	}
	if err = s.ApplyV2(ctx, original); err != nil {
		t.Fatalf("exact replay rejected: %v", err)
	}
	for _, change := range []string{"version", "payload", "aggregate", "type"} {
		t.Run(change, func(t *testing.T) {
			changed := original
			switch change {
			case "version":
				changed.AggregateVersion = 2
			case "payload":
				changed.Payload, _ = json.Marshal(eventsv2.ProfileUpdated{UserID: user, WebsiteLocale: "ru", NotificationLocale: "en", Version: 1})
			case "aggregate":
				changed.AggregateID = uuid.NewString()
				changed.Payload, _ = json.Marshal(eventsv2.ProfileUpdated{UserID: changed.AggregateID, WebsiteLocale: "en", NotificationLocale: "en", Version: 1})
			case "type":
				changed.EventType = "profile.telegram_binding_changed"
				changed.Payload, _ = json.Marshal(eventsv2.TelegramBindingChanged{UserID: user, BindingVersion: 1, Linked: true})
			}
			if err := changed.Validate(); err != nil {
				t.Fatalf("collision fixture invalid: %v", err)
			}
			if err := s.ApplyV2(ctx, changed); !errors.Is(err, platform.ErrPoison) {
				t.Fatalf("changed immutable event silently acknowledged: %v", err)
			}
		})
	}
	var version int64
	if err = db.QueryRow(ctx, "SELECT version FROM audit_aggregate_versions WHERE producer='profile' AND aggregate_type='user' AND aggregate_id=$1", user).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("collision advanced cursor: %d", version)
	}
	next := original
	nextID, err := uuid.NewV7()
	if err != nil {
		t.Fatal(err)
	}
	next.EventID = nextID.String()
	next.AggregateVersion = 2
	if err = s.ApplyV2(ctx, next); err != nil {
		t.Fatalf("collision blocked next legitimate event: %v", err)
	}
}

func TestIntegrationV1ReceiptTypeCollisionIsPoison(t *testing.T) {
	db := auditDatabase(t)
	p := &PostgresAudit{DB: db}
	ctx := context.Background()
	e := platform.Envelope{EventID: uuid.NewString(), EventType: "identity.email_verified", AggregateID: uuid.NewString(), OccurredAt: time.Now().UTC()}
	if err := p.Project(ctx, e); err != nil {
		t.Fatal(err)
	}
	e.EventType = "identity.password_changed"
	if err := p.Project(ctx, e); !errors.Is(err, platform.ErrPoison) {
		t.Fatalf("legacy changed event type acknowledged: %v", err)
	}
}
