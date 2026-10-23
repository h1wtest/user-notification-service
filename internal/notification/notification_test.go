package notification

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"testing"
	"time"
	"user-notification-service/internal/platform"
)

// TEST_DATABASE_URL must point at a disposable notification database. Tests use
// unique UUIDs and never truncate tables or touch an existing environment.
func testDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL required for PostgreSQL behavior regressions")
	}
	db, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err = platform.Migrate(context.Background(), db, "notification"); err != nil {
		t.Fatal(err)
	}
	return db
}
func TestDuplicateIntentCreatesOneJob(t *testing.T) {
	db := testDB(t)
	s := New(db, platform.Config{}, nil)
	id, user := uuid.NewString(), uuid.NewString()
	t.Cleanup(func() { _, _ = db.Exec(context.Background(), "DELETE FROM delivery_jobs WHERE event_id=$1", id) })
	payload, _ := json.Marshal(map[string]any{"user_id": user, "email": "test@example.test", "code": "876543", "expires_at": time.Now().Add(time.Hour)})
	cfg := platform.Config{DeliveryKey: make([]byte, 32)}
	s.Config = cfg
	encrypted, err := platform.Encrypt(cfg.DeliveryKey, id, payload)
	if err != nil {
		t.Fatal(err)
	}
	e := platform.Envelope{SchemaVersion: 1, EventID: id, AggregateID: user, EventType: "notification.email_verification", OccurredAt: time.Now(), EncryptedPayload: encrypted}
	for range 2 {
		if err = s.Apply(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err = db.QueryRow(context.Background(), "SELECT count(*) FROM delivery_jobs WHERE event_id=$1", id).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("duplicate produced %d jobs", count)
	}
}
func TestFailedDeliveryPersistsRetryAndCannotLeaseTwice(t *testing.T) {
	db := testDB(t)
	s := New(db, platform.Config{DeliveryKey: make([]byte, 32)}, nil)
	id, user := uuid.NewString(), uuid.NewString()
	t.Cleanup(func() {
		_, _ = db.Exec(context.Background(), "DELETE FROM delivery_attempts WHERE job_id=$1", id)
		_, _ = db.Exec(context.Background(), "DELETE FROM delivery_jobs WHERE id=$1", id)
	})
	body, _ := platform.Encrypt(s.Config.DeliveryKey, id, []byte(`{"to":"test@example.test","subject":"test","body":"test"}`))
	_, err := db.Exec(context.Background(), "INSERT INTO delivery_jobs(id,event_id,user_id,channel,encrypted_body,message_id) VALUES($1,$1,$2,'email',$3,$4)", id, user, body, "<"+id+"@sso.local>")
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.Jobs.Claim(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// Another live lease must not claim this job.
	var count int
	if err = db.QueryRow(context.Background(), "SELECT count(*) FROM delivery_jobs WHERE id=$1 AND status='leased' AND lease_until>now()", job.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("lease not durable: %v", err)
	}
	if err = s.Jobs.Finish(context.Background(), job, false, false, "temporary provider failure"); err != nil {
		t.Fatal(err)
	}
	var status string
	var attempts int
	var future bool
	if err = db.QueryRow(context.Background(), "SELECT status,attempts,next_attempt_at>now() FROM delivery_jobs WHERE id=$1", job.ID).Scan(&status, &attempts, &future); err != nil {
		t.Fatal(err)
	}
	if status != "retry" || attempts != 1 || !future {
		t.Fatalf("lost retry state: %s %d %v", status, attempts, future)
	}
}
func TestCiphertextRejectsDifferentEventIdentity(t *testing.T) {
	key := make([]byte, 32)
	encrypted, err := platform.Encrypt(key, "event-a", []byte("verification-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = platform.Decrypt(key, "event-b", encrypted); err == nil {
		t.Fatal("ciphertext accepted different event identity")
	}
}
