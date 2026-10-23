package platform

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMalformedExplicitAuthorizationNeverFallsBackToCookie(t *testing.T) {
	c := &Client{}
	r := httptest.NewRequest("GET", "https://app.test/api/v1/users/me", nil)
	r.Header.Set("Authorization", "Basic ignored-secret")
	r.AddCookie(&http.Cookie{Name: "sso_session", Value: "valid-session"})
	w := httptest.NewRecorder()
	if _, ok := c.Authenticate(w, r, "", false); ok || w.Code != 401 {
		t.Fatal("malformed explicit credentials bypassed")
	}
}
