package notification

import (
	"context"
	"github.com/google/uuid"
	"testing"
	"time"
	"user-notification-service/internal/platform"
)

func TestExpiredLeaseRejectsOldWorkerCompletion(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	id, user := uuid.NewString(), uuid.NewString()
	s := New(db, platform.Config{}, nil)
	t.Cleanup(func() {
		_, _ = db.Exec(ctx, "DELETE FROM delivery_attempts WHERE job_id=$1", id)
		_, _ = db.Exec(ctx, "DELETE FROM delivery_jobs WHERE id=$1", id)
	})
	_, err := db.Exec(ctx, "INSERT INTO delivery_jobs(id,event_id,user_id,channel,encrypted_body,message_id,next_attempt_at) VALUES($1,$1,$2,'email','encrypted','<stable@sso.local>',$3)", id, user, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	first, err := s.Jobs.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != id {
		t.Fatal("disposable database must not contain unrelated pending jobs")
	}
	if _, err = db.Exec(ctx, "UPDATE delivery_jobs SET lease_until=now()-interval '1 second' WHERE id=$1", id); err != nil {
		t.Fatal(err)
	}
	second, err := s.Jobs.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.LeaseToken == second.LeaseToken {
		t.Fatal("reclaim reused lease token")
	}
	if err = s.Jobs.Finish(ctx, first, true, false, ""); err == nil {
		t.Fatal("expired worker overwrote replacement lease")
	}
	if err = s.Jobs.Finish(ctx, second, true, false, ""); err != nil {
		t.Fatal(err)
	}
	var status string
	var ambiguous bool
	if err = db.QueryRow(ctx, "SELECT status,ambiguous FROM delivery_jobs WHERE id=$1", id).Scan(&status, &ambiguous); err != nil || status != "sent" || !ambiguous {
		t.Fatalf("crash ambiguity lost: %s %v %v", status, ambiguous, err)
	}
}
