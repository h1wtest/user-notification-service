package platform

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCookieOnlyForwardingAuthenticatesProtectedHTTPResource(t *testing.T) {
	// A real TLS HTTP authentication boundary: explicitly supplied bearer
	// credentials select that scheme; otherwise a stored session authorizes
	// access. This reproduces the identity boundary's header-presence rule.
	sessions := map[string]string{"owned-session": "owned-user"}
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, present := r.Header["Authorization"]; present {
			http.Error(w, "invalid explicit bearer", http.StatusUnauthorized)
			return
		}
		if _, present := r.Header["X-Actor-Token"]; present {
			http.Error(w, "invalid alternate bearer", http.StatusUnauthorized)
			return
		}
		cookie, err := r.Cookie("sso_session")
		if err != nil || sessions[cookie.Value] == "" {
			http.Error(w, "session not found", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sessions[cookie.Value]))
	}))
	defer target.Close()
	client := &Client{HTTP: target.Client()}
	source := httptest.NewRequest("GET", "https://browser.test/profile", nil)
	source.AddCookie(&http.Cookie{Name: "sso_session", Value: "owned-session"})
	status, body, err := client.Call(context.Background(), "GET", target.URL+"/protected", nil, source)
	if err != nil || status != http.StatusOK || string(body) != "owned-user" {
		t.Fatalf("cookie-only authentication failed: status=%d body=%s err=%v", status, body, err)
	}

	source.Header.Set("Authorization", "")
	status, _, err = client.Call(context.Background(), "GET", target.URL+"/protected", nil, source)
	if err != nil || status != http.StatusUnauthorized {
		t.Fatalf("explicit empty credentials disappeared: status=%d err=%v", status, err)
	}
}

func TestExplicitEmptyAuthorizationCannotFallBackToCookie(t *testing.T) {
	source := httptest.NewRequest("GET", "https://browser.test/profile", nil)
	source.AddCookie(&http.Cookie{Name: "sso_session", Value: "owned-session"})
	source.Header.Set("Authorization", "")
	response := httptest.NewRecorder()
	if _, ok := (&Client{}).Authenticate(response, source, "", false); ok || response.Code != http.StatusUnauthorized {
		t.Fatalf("explicit empty authorization accepted: status=%d", response.Code)
	}
}
