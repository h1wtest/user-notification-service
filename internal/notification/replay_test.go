package notification

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"user-notification-service/internal/platform"
)

func TestPoisonCorrectionReplaysOnceWithDurableActorAudit(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	key := make([]byte, 32)
	actorID := uuid.NewString()
	identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		platform.JSON(w, 200, platform.Actor{Active: true, UserID: actorID, Email: "admin@example.test", SessionID: "019a0000-0000-7000-8000-000000000004", Roles: []string{"admin"}, MFAVerified: true, Permissions: []string{"notifications:replay"}})
	}))
	defer identity.Close()
	c := platform.Config{DeliveryKey: key, QuarantineKey: make([]byte, 32), IdentityURL: identity.URL, AdminToken: "admin-client"}
	client := &platform.Client{Config: c, HTTP: identity.Client()}
	s := New(db, c, client)
	partition := int32(43)
	offset := time.Now().UnixNano()
	if err := platform.Quarantine(ctx, db, c, "identity.events.v1", partition, offset, []byte("invalid json"), "invalid_event"); err != nil {
		t.Fatal(err)
	}
	var id string
	if err := db.QueryRow(ctx, "SELECT id FROM quarantine WHERE topic='identity.events.v1' AND partition_id=$1 AND offset_id=$2", partition, offset).Scan(&id); err != nil {
		t.Fatal(err)
	}
	eventID, user := uuid.NewString(), uuid.NewString()
	payload, _ := json.Marshal(map[string]string{"user_id": user})
	event := platform.Envelope{SchemaVersion: 1, EventID: eventID, EventType: "identity.email_verified", AggregateID: user, OccurredAt: time.Now(), Payload: payload}
	body, err := json.Marshal(map[string]any{"corrected_event": event, "reason": "correct poisoned event"})
	if err != nil {
		t.Fatal(err)
	}
	m := http.NewServeMux()
	s.Routes(m)
	request := func(body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "https://notification.test/internal/v1/quarantine/"+id+"/replay", strings.NewReader(body))
		r.Header.Set("X-Service-Token", "admin-client")
		r.AddCookie(&http.Cookie{Name: "sso_session", Value: "real-admin-session"})
		w := httptest.NewRecorder()
		m.ServeHTTP(w, r)
		return w
	}
	if w := request(`{"reason":"retry original"}`); w.Code != 409 {
		t.Fatalf("invalid original reported successful replay: %d", w.Code)
	}
	if w := request(string(body)); w.Code != 200 {
		t.Fatalf("corrected replay failed: %d %s", w.Code, w.Body.String())
	}
	if w := request(string(body)); w.Code != 404 {
		t.Fatalf("completed replay not fenced: %d", w.Code)
	}
	var n int
	if err = db.QueryRow(ctx, "SELECT count(*) FROM quarantine_replays WHERE quarantine_id=$1 AND actor_id=$2 AND completed_at IS NOT NULL", id, actorID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("replay audit absent: %d %v", n, err)
	}
}
