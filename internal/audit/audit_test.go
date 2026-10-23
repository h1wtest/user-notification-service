package audit

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"user-notification-service/internal/platform"
)

func TestAdminRequiresPermissionAndLiveMFA(t *testing.T) {
	for _, tc := range []struct {
		name  string
		actor platform.Actor
	}{
		{"missing_mfa", platform.Actor{Active: true, UserID: "019a0000-0000-7000-8000-000000000002", Email: "admin@example.test", SessionID: "019a0000-0000-7000-8000-000000000004", Roles: []string{"admin"}, Permissions: []string{"users:write"}}},
		{"missing_permission", platform.Actor{Active: true, UserID: "019a0000-0000-7000-8000-000000000002", Email: "admin@example.test", SessionID: "019a0000-0000-7000-8000-000000000004", Roles: []string{"admin"}, Permissions: []string{}, MFAVerified: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { platform.JSON(w, 200, tc.actor) }))
			defer identity.Close()
			s := &Service{Client: &platform.Client{Config: platform.Config{IdentityURL: identity.URL}, HTTP: identity.Client()}}
			m := http.NewServeMux()
			s.Routes(m)
			r := httptest.NewRequest("POST", "https://app.test/api/v1/admin/users/019a0000-0000-7000-8000-000000000003/ban", strings.NewReader(`{"reason":"test"}`))
			r.AddCookie(&http.Cookie{Name: "sso_session", Value: "session"})
			w := httptest.NewRecorder()
			m.ServeHTTP(w, r)
			if w.Code != 403 {
				t.Fatalf("unauthorized command reached database: %d", w.Code)
			}
		})
	}
}
