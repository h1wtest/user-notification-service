package platform

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestQuarantineMetadataUsesCanonicalUTCTimestamps(t *testing.T) {
	db := nestedRekeyDB(t)
	ctx := context.Background()
	id := uuid.NewString()
	user := uuid.NewString()
	session := uuid.NewString()
	if _, err := db.Exec(ctx, "INSERT INTO quarantine(id,topic,partition_id,offset_id,encrypted_record,reason,replayed_at) VALUES($1,'identity.events.v2',1,1,'ciphertext','invalid_event',now())", id); err != nil {
		t.Fatal(err)
	}
	identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		JSON(w, 200, Actor{Active: true, UserID: user, Email: "staff@example.invalid", SessionID: session, Roles: []string{"admin"}, Permissions: []string{"notifications:read"}, MFAVerified: true})
	}))
	defer identity.Close()
	q := QuarantineAPI{DB: db, Client: &Client{Config: Config{IdentityURL: identity.URL}, HTTP: identity.Client()}, Owner: "notification", Local: true}
	request := httptest.NewRequest("GET", "https://app.invalid/api/v1/admin/quarantine", nil)
	request.AddCookie(&http.Cookie{Name: "sso_session", Value: "session"})
	response := httptest.NewRecorder()
	q.List(response, request)
	if response.Code != 200 {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var items []struct {
		CreatedAt  string `json:"created_at"`
		ReplayedAt string `json:"replayed_at"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &items); err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("items=%d", len(items))
	}
	for _, value := range []string{items[0].CreatedAt, items[0].ReplayedAt} {
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil || value[len(value)-1] != 'Z' || parsed.Location() != time.UTC {
			t.Fatalf("noncanonical UTC timestamp %q (%v)", value, err)
		}
	}
}
