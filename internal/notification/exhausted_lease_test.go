package notification

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"user-notification-service/internal/platform"
)

func TestRecoveredEighthLeaseFailsWithoutNinthSend(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	id, user := uuid.NewString(), uuid.NewString()
	_, err := db.Exec(ctx, "INSERT INTO delivery_jobs(id,event_id,user_id,channel,encrypted_body,message_id,status,attempts,lease_until,lease_token) VALUES($1,$1,$2,'email','encrypted','<stable@sso.local>','leased',8,now()-interval '1 second',$3)", id, user, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(ctx, "DELETE FROM delivery_attempts WHERE job_id=$1", id)
		_, _ = db.Exec(ctx, "DELETE FROM delivery_jobs WHERE id=$1", id)
	})
	s := New(db, platform.Config{}, nil)
	job, err := s.Jobs.Claim(ctx)
	if err == nil && job.ID == id {
		t.Fatal("ninth provider attempt was leased")
	}
	var status string
	var attempts int
	var ambiguous bool
	if err = db.QueryRow(ctx, "SELECT status,attempts,ambiguous FROM delivery_jobs WHERE id=$1", id).Scan(&status, &attempts, &ambiguous); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || attempts != 8 || !ambiguous {
		t.Fatalf("recovered terminal state lost: %s %d %v", status, attempts, ambiguous)
	}
}
