package platform

import (
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestIntrospectionDenialCannotBecomeAdminAccess(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403) }))
	defer server.Close()
	c := &Client{Config: Config{IdentityURL: server.URL, Token: "internal"}, HTTP: server.Client()}
	r := httptest.NewRequest("GET", "https://app.test/api/v1/admin/users", nil)
	r.AddCookie(&http.Cookie{Name: "sso_session", Value: "revoked-session"})
	w := httptest.NewRecorder()
	if _, ok := c.Authenticate(w, r, "users:read", true); ok || w.Code != 403 {
		t.Fatalf("identity denial bypassed: %d", w.Code)
	}
}
func TestForeignIdentityFromBrowserIsNotAccepted(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["session_cookie"] != "owned-session" || body["user_id"] != nil {
			t.Error("caller identity was not bound to session")
		}
		JSON(w, 200, Actor{Active: true, UserID: "019a0000-0000-7000-8000-000000000002", Email: "authoritative@example.test", SessionID: "019a0000-0000-7000-8000-000000000004", Roles: []string{}, Permissions: []string{}})
	}))
	defer server.Close()
	c := &Client{Config: Config{IdentityURL: server.URL, Token: "internal"}, HTTP: server.Client()}
	r := httptest.NewRequest("GET", "https://app.test/?user_id=foreign-user", nil)
	r.AddCookie(&http.Cookie{Name: "sso_session", Value: "owned-session"})
	a, ok := c.Authenticate(httptest.NewRecorder(), r, "", false)
	if !ok || a.UserID != "019a0000-0000-7000-8000-000000000002" {
		t.Fatal("untrusted identity accepted")
	}
}

func TestIntrospectionRejectsMalformedSuccessfulActor(t *testing.T) {
	for _, tc := range []struct{ name, response string }{
		{"partial actor", `{"active":true,"user_id":"019a0000-0000-7000-8000-000000000002"}`},
		{"null roles", `{"active":true,"user_id":"019a0000-0000-7000-8000-000000000002","email":"actor@example.test","session_id":"019a0000-0000-7000-8000-000000000004","roles":null,"permissions":[],"mfa_verified":false}`},
		{"null permissions", `{"active":true,"user_id":"019a0000-0000-7000-8000-000000000002","email":"actor@example.test","session_id":"019a0000-0000-7000-8000-000000000004","roles":[],"permissions":null,"mfa_verified":false}`},
		{"invalid session", `{"active":true,"user_id":"019a0000-0000-7000-8000-000000000002","email":"actor@example.test","session_id":"not-a-uuid","roles":[],"permissions":[],"mfa_verified":false}`},
		{"unknown field", `{"active":true,"user_id":"019a0000-0000-7000-8000-000000000002","email":"actor@example.test","session_id":"019a0000-0000-7000-8000-000000000004","roles":[],"permissions":[],"mfa_verified":false,"actor_id":"injected"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.response))
			}))
			defer identity.Close()
			client := &Client{Config: Config{IdentityURL: identity.URL}, HTTP: identity.Client()}
			r := httptest.NewRequest(http.MethodGet, "https://app.test/api/v1/users/me", nil)
			r.AddCookie(&http.Cookie{Name: "sso_session", Value: "session"})
			w := httptest.NewRecorder()
			if _, ok := client.Authenticate(w, r, "", false); ok || w.Code != http.StatusUnauthorized {
				t.Fatalf("malformed identity response accepted: status %d", w.Code)
			}
		})
	}
}
func TestPoisonRecordIsDurablyQuarantinedWithoutPlaintext(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("disposable TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = Migrate(ctx, db, "notification"); err != nil {
		t.Fatal(err)
	}
	c := Config{QuarantineKey: make([]byte, 32)}
	raw := []byte(`{"verification-secret":"do-not-store-plaintext"}`)
	if err = Quarantine(ctx, db, c, "notification.commands.v1", 42, 1234567, raw, "invalid_envelope"); err != nil {
		t.Fatal(err)
	}
	var encrypted string
	if err = db.QueryRow(ctx, "SELECT encrypted_record FROM quarantine WHERE topic=$1 AND partition_id=42 AND offset_id=1234567", "notification.commands.v1").Scan(&encrypted); err != nil {
		t.Fatal(err)
	}
	decoded, err := Decrypt(c.QuarantineKey, "notification.commands.v1:42:1234567", encrypted)
	if err != nil || string(decoded) != string(raw) {
		t.Fatal("quarantine lost original record")
	}
	if encrypted == string(raw) {
		t.Fatal("quarantine stored plaintext")
	}
}
