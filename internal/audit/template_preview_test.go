package audit

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"user-notification-service/internal/platform"
)

func TestTemplatePreviewProxyUsesCurrentWriteAuthorityAndTypedOwner(t *testing.T) {
	allowed := true
	ownerCalls := 0
	body := `{"locales":{"en":{"subject":"Preview","body":"{{period_end}}"},"ru":{"subject":"Просмотр","body":"{{period_end}}"}}}`
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/v2/introspect" {
			var in struct {
				Permission string `json:"permission"`
				MFA        bool   `json:"require_mfa"`
				Method     string `json:"method"`
				Origin     string `json:"origin"`
				CSRF       string `json:"csrf_token"`
			}
			if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
				t.Error(err)
			}
			if in.Permission != "notifications:templates:write" || !in.MFA || in.Method != "POST" || in.Origin != "https://accounts.example.test" || in.CSRF != "csrf" {
				t.Error("proxy lost current mutation authority")
			}
			if !allowed {
				platform.Error(w, 403, "access_denied")
				return
			}
			platform.JSON(w, 200, platform.Actor{Active: true, UserID: uuid.NewString(), SessionID: uuid.NewString(), Email: "admin@example.test", Roles: []string{"admin"}, Permissions: []string{"notifications:templates:write"}, MFAVerified: true})
			return
		}
		if r.URL.Path != "/internal/v2/templates/subscription.changed/preview" || r.Method != "POST" {
			w.WriteHeader(404)
			return
		}
		ownerCalls++
		if r.Header.Get("X-Service-Token") != "admin-test" || r.Header.Get("Origin") != "https://accounts.example.test" || r.Header.Get("X-CSRF-Token") != "csrf" {
			t.Error("preview lost workload/actor authentication")
		}
		platform.JSON(w, 200, map[string]any{"locales": map[string]any{"en": map[string]string{"subject": "Preview", "body": "2030-01-02T03:04:05Z"}, "ru": map[string]string{"subject": "Просмотр", "body": "2030-01-02T03:04:05Z"}}})
	}))
	defer server.Close()
	c := platform.Config{IdentityURL: server.URL, NotificationURL: server.URL, Token: "admin-test"}
	s := &Service{Config: c, Client: &platform.Client{Config: c, HTTP: server.Client()}}
	mux := http.NewServeMux()
	s.Routes(mux)
	request := func(raw string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "https://accounts.example.test/api/v1/admin/templates/subscription.changed/preview", strings.NewReader(raw))
		r.AddCookie(&http.Cookie{Name: "sso_session", Value: "session"})
		r.Header.Set("Origin", "https://accounts.example.test")
		r.Header.Set("X-CSRF-Token", "csrf")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}
	if w := request(body); w.Code != 200 || !strings.Contains(w.Body.String(), "2030-01-02T03:04:05Z") {
		t.Fatalf("typed preview proxy missing: %d %s", w.Code, w.Body.String())
	}
	if w := request(strings.TrimSuffix(body, "}") + `,"examples":{}}`); w.Code != 400 || ownerCalls != 1 {
		t.Fatal("arbitrary preview inputs reached owner", w.Code, ownerCalls)
	}
	allowed = false
	if w := request(body); w.Code != 403 || ownerCalls != 1 {
		t.Fatal("unauthorized preview reached owner", w.Code, ownerCalls)
	}
}
