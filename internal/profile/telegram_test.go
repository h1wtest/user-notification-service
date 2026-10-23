package profile

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"user-notification-service/internal/platform"
)

func TestMissingBotCredentialsNeverCreateFakeLink(t *testing.T) {
	identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		platform.JSON(w, 200, platform.Actor{Active: true, UserID: "019a0000-0000-7000-8000-000000000002", Email: "profile@example.test", SessionID: "019a0000-0000-7000-8000-000000000004", Roles: []string{}, Permissions: []string{}})
	}))
	defer identity.Close()
	s := &Service{Client: &platform.Client{Config: platform.Config{IdentityURL: identity.URL}, HTTP: identity.Client()}}
	m := http.NewServeMux()
	s.Routes(m)
	r := httptest.NewRequest("POST", "https://app.test/api/v1/users/me/telegram/link", nil)
	r.AddCookie(&http.Cookie{Name: "sso_session", Value: "real-session"})
	w := httptest.NewRecorder()
	m.ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatalf("unconfigured bot returned fake link: %d", w.Code)
	}
}
