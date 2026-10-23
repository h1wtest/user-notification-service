package notification

import (
	"context"
	"github.com/google/uuid"
	"testing"
	"user-notification-service/internal/platform"
)

func TestUnavailablePolicyDoesNotExhaustProviderAttempts(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	id, user := uuid.NewString(), uuid.NewString()
	s := New(db, platform.Config{}, nil)
	cleanupDelivery(t, db, id)
	if _, err := db.Exec(ctx, "INSERT INTO delivery_jobs(id,event_id,user_id,channel,encrypted_body,message_id,attempts,status,lease_token) VALUES($1,$1,$2,'email','encrypted','<test@example.test>',8,'leased',$3)", id, user, id); err != nil {
		t.Fatal(err)
	}
	if err := s.Jobs.Finish(ctx, Job{ID: id, LeaseToken: id, Attempts: 8, Deferred: true}, false, false, "recipient_policy_unavailable"); err != nil {
		t.Fatal(err)
	}
	var status string
	var attempts int
	if err := db.QueryRow(ctx, "SELECT status,attempts FROM delivery_jobs WHERE id=$1", id).Scan(&status, &attempts); err != nil || status != "retry" || attempts != 7 {
		t.Fatalf("authority outage consumed provider attempt: %s %d %v", status, attempts, err)
	}
}
