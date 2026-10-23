package notification

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"user-notification-service/internal/platform"
)

func TestTelegramRetryAfterIsPreserved(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		_, _ = w.Write([]byte(`{"ok":false,"error_code":429,"parameters":{"retry_after":73}}`))
	}))
	defer server.Close()
	s := New(nil, platform.Config{TelegramToken: "test-token", TelegramBase: server.URL}, &platform.Client{HTTP: server.Client()})
	permanent, reason, err := s.sendTelegram(context.Background(), Job{}, Body{To: "123", Subject: "test", Text: "test"})
	var retry *providerRetry
	if permanent || reason != "telegram_rate_limited" || !errors.As(err, &retry) || retry.After != 73*time.Second {
		t.Fatalf("retry_after lost: %v %s %v", permanent, reason, err)
	}
}

func TestTelegramRetryAfterPersistsProviderDeadline(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	id, user := uuid.NewString(), uuid.NewString()
	cleanupDelivery(t, db, id)
	if _, err := db.Exec(ctx, "INSERT INTO delivery_jobs(id,event_id,user_id,channel,encrypted_body,message_id,status,lease_token,attempts) VALUES($1,$1,$2,'telegram','encrypted','<test@example.test>','leased',$1,1)", id, user); err != nil {
		t.Fatal(err)
	}
	s := New(db, platform.Config{}, nil)
	before := time.Now().Add(72 * time.Second)
	if err := s.Jobs.Finish(ctx, Job{ID: id, LeaseToken: id, Attempts: 1, RetryAfter: 73 * time.Second}, false, false, "telegram_rate_limited"); err != nil {
		t.Fatal(err)
	}
	var next time.Time
	if err := db.QueryRow(ctx, "SELECT next_attempt_at FROM delivery_jobs WHERE id=$1", id).Scan(&next); err != nil || next.Before(before) {
		t.Fatalf("provider retry deadline lost: %v %v", next, err)
	}
}
