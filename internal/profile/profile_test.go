package profile

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"user-notification-service/internal/platform"
)

func TestProfileRejectsCallerSuppliedTelegramIdentity(t *testing.T) {
	identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		platform.JSON(w, 200, platform.Actor{Active: true, UserID: "019a0000-0000-7000-8000-000000000002", Email: "profile@example.test", SessionID: "019a0000-0000-7000-8000-000000000004", Roles: []string{}, Permissions: []string{}})
	}))
	defer identity.Close()
	s := &Service{Client: &platform.Client{Config: platform.Config{IdentityURL: identity.URL, Token: "profile-client"}, HTTP: identity.Client()}}
	mux := http.NewServeMux()
	s.Routes(mux)
	r := httptest.NewRequest("PATCH", "https://app.test/api/v1/users/me", strings.NewReader(`{"telegram_id":"victim-chat"}`))
	r.AddCookie(&http.Cookie{Name: "sso_session", Value: "real-session"})
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatalf("arbitrary chat identity accepted: %d", w.Code)
	}
}
func TestTelegramWebhookRejectsUnverifiedSenderBeforeDatabase(t *testing.T) {
	s := &Service{Config: platform.Config{TelegramToken: "configured-token", TelegramWebhookSecret: "expected-secret"}}
	m := http.NewServeMux()
	s.Routes(m)
	r := httptest.NewRequest("POST", "https://app.test/internal/v1/telegram/webhook", strings.NewReader(`{"update_id":1}`))
	w := httptest.NewRecorder()
	m.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("unverified webhook accepted: %d", w.Code)
	}
}
func TestRevokedSessionCannotReachProfileDatabase(t *testing.T) {
	identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { platform.Error(w, 401, "revoked") }))
	defer identity.Close()
	s := &Service{Client: &platform.Client{Config: platform.Config{IdentityURL: identity.URL}, HTTP: identity.Client()}}
	m := http.NewServeMux()
	s.Routes(m)
	r := httptest.NewRequest("GET", "https://app.test/api/v1/users/me", nil)
	r.AddCookie(&http.Cookie{Name: "sso_session", Value: "revoked"})
	w := httptest.NewRecorder()
	m.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatalf("revoked session accepted: %d", w.Code)
	}
}

func TestProfilePatchRejectsMalformedAndInvalidFields(t *testing.T) {
	identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		platform.JSON(w, http.StatusOK, platform.Actor{Active: true, UserID: "019a0000-0000-7000-8000-000000000002", Email: "profile@example.test", SessionID: "019a0000-0000-7000-8000-000000000004", Roles: []string{}, Permissions: []string{}})
	}))
	defer identity.Close()
	s := &Service{Client: &platform.Client{Config: platform.Config{IdentityURL: identity.URL}, HTTP: identity.Client()}}
	mux := http.NewServeMux()
	s.Routes(mux)
	assertRejected := func(t *testing.T, body string) {
		t.Helper()
		r := httptest.NewRequest(http.MethodPatch, "https://app.test/api/v1/users/me", strings.NewReader(body))
		r.AddCookie(&http.Cookie{Name: "sso_session", Value: "profile-patch-session"})
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("PATCH returned %d, want 400: %s", w.Code, w.Body.String())
		}
	}
	for _, field := range []string{"display_name", "avatar_url", "phone_number"} {
		for _, value := range []string{`true`, `42`, `{}`, `[]`} {
			t.Run(field+"/"+value, func(t *testing.T) {
				assertRejected(t, `{"`+field+`":`+value+`}`)
			})
		}
	}
	for _, tc := range []struct{ name, body string }{
		{"unknown alongside null", `{"display_name":null,"unexpected":null}`},
		{"oversized display", `{"display_name":"` + strings.Repeat("d", 101) + `"}`},
		{"oversized phone", `{"phone_number":"` + strings.Repeat("1", 31) + `"}`},
		{"oversized avatar", `{"avatar_url":"https://example.test/` + strings.Repeat("a", 2048) + `"}`},
		{"http avatar", `{"avatar_url":"http://example.test/avatar.png"}`},
		{"hostless avatar", `{"avatar_url":"https:avatar.png"}`},
		{"unsafe avatar", `{"avatar_url":"javascript:alert(1)"}`},
		{"trailing document", `{"display_name":null} {}`},
	} {
		t.Run(tc.name, func(t *testing.T) { assertRejected(t, tc.body) })
	}
}
