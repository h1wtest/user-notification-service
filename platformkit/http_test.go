package platformkit

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeRejectsAmbiguousAndUnboundedBodies(t *testing.T) {
	for _, raw := range []string{"", "null", `{"name":"a","extra":1}`, `{"name":"a"} {}`, `{"name":"a","name":"b"}`, `{"name":"a","na\u006de":"b"}`, `{"name":"a","NAME":"b"}`, `{"NAME":"b"}`, `{"name":"` + strings.Repeat("x", 1<<20) + `"}`} {
		t.Run(raw[:min(20, len(raw))], func(t *testing.T) {
			r := httptest.NewRequest("POST", "/", strings.NewReader(raw))
			w := httptest.NewRecorder()
			var body struct {
				Name string `json:"name"`
			}
			if Decode(w, r, &body) || w.Code != http.StatusBadRequest {
				t.Fatalf("accepted invalid body: status=%d", w.Code)
			}
		})
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/", strings.NewReader(`{"name":"ok"}`))
	var body struct {
		Name string `json:"name"`
	}
	if !Decode(w, r, &body) || body.Name != "ok" {
		t.Fatal("valid body rejected")
	}
}

func TestDecodeKeepsExactNestedFieldsAndCaseSensitiveMapKeys(t *testing.T) {
	type input struct {
		Nested struct {
			Name string `json:"name"`
		} `json:"nested"`
		Labels map[string]string `json:"labels"`
	}
	for _, raw := range []string{`{"nested":{"NAME":"forged"}}`, `{"nested":{"name":"a","Name":"b"}}`} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/", strings.NewReader(raw))
		var value input
		if Decode(w, r, &value) || w.Code != 400 {
			t.Fatal("nested case alias admitted")
		}
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/", strings.NewReader(`{"nested":{"name":"ok"},"labels":{"Name":"one","name":"two"}}`))
	var value input
	if !Decode(w, r, &value) || value.Labels["Name"] != "one" || value.Labels["name"] != "two" {
		t.Fatal("distinct map keys incorrectly case-folded")
	}
}

func TestAuthenticateFailsClosedAndUsesV2Decision(t *testing.T) {
	const valid = `{"active":true,"user_id":"019a1234-1234-7123-8123-123456789abc","session_id":"019a1234-1234-7123-8123-123456789abd","email":"a@example.test","roles":["admin"],"permissions":["catalog:write"],"mfa_verified":true,"is_verified":true}`
	cases := []struct {
		name, body   string
		status, want int
	}{
		{"authorized", valid, 200, 200},
		{"inactive", strings.Replace(valid, `"active":true`, `"active":false`, 1), 200, 401},
		{"missing_permission", strings.Replace(valid, `"catalog:write"`, `"catalog:read"`, 1), 200, 403},
		{"missing_mfa", strings.Replace(valid, `"mfa_verified":true`, `"mfa_verified":false`, 1), 200, 403},
		{"malformed", `{"active":true}`, 200, 503},
		{"dependency", "", 503, 503},
		{"forbidden", "", 403, 403},
		{"unauthorized", "", 401, 401},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/internal/v2/introspect" || r.Header.Get("X-Service-Token") != "workload" {
					t.Error("incorrect identity admission boundary")
				}
				var req struct {
					SessionCookie string `json:"session_cookie"`
					AccessToken   string `json:"access_token"`
					CSRF          string `json:"csrf_token"`
					Method        string `json:"method"`
					Origin        string `json:"origin"`
					Permission    string `json:"permission"`
					MFA           bool   `json:"require_mfa"`
				}
				if json.NewDecoder(r.Body).Decode(&req) != nil || req.SessionCookie != "real-session" || req.Method != "PUT" || req.Permission != "catalog:write" || !req.MFA || req.CSRF != "csrf" || req.Origin != "https://public.test" {
					t.Error("identity did not receive original admission context")
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			client := &Client{HTTP: server.Client(), Config: Config{IdentityURL: server.URL, Token: "workload"}}
			r := httptest.NewRequest("PUT", "https://public.test/api/v1/admin/catalog", nil)
			r.AddCookie(&http.Cookie{Name: "sso_session", Value: "real-session"})
			r.Header.Set("X-CSRF-Token", "csrf")
			r.Header.Set("Origin", "https://public.test")
			w := httptest.NewRecorder()
			actor, ok := client.Authenticate(w, r, "catalog:write", true)
			if ok != (tc.want == 200) || w.Code != tc.want {
				t.Fatalf("ok=%v status=%d want=%d", ok, w.Code, tc.want)
			}
			if ok && (!actor.Verified || actor.UserID == "") {
				t.Fatal("authoritative actor fields lost")
			}
			if !ok && actor.UserID != "" {
				t.Fatal("failed admission returned actor")
			}
		})
	}
}

func TestAuthenticateRejectsBrowserActorForgeryAndMalformedBearer(t *testing.T) {
	for _, authorization := range []string{"", "Basic abc", "Bearer ", "Bearer a b"} {
		r := httptest.NewRequest("POST", "https://public.test/api/v1/orders", nil)
		r.Header.Set("X-Actor-Session", "forged")
		r.Header.Set("X-Service-Token", "forged")
		if authorization != "" {
			r.Header.Set("Authorization", authorization)
		}
		w := httptest.NewRecorder()
		client := &Client{Config: Config{IdentityURL: "https://127.0.0.1:1", Token: "workload"}, HTTP: http.DefaultClient}
		if _, ok := client.Authenticate(w, r, "", false); ok || w.Code != 401 {
			t.Fatalf("forgery status=%d", w.Code)
		}
	}
}

func TestForwardActorRemovesForgedCredentialsAndPreservesTrustedInternalHop(t *testing.T) {
	browser := httptest.NewRequest("GET", "https://public.test/api/v1/orders", nil)
	browser.Header.Set("X-Actor-Session", "forged")
	browser.Header.Set("X-Actor-Token", "forged")
	browser.Header.Set("X-Actor-User", "forged")
	dst := httptest.NewRequest("GET", "https://internal.test/internal/v2/orders", nil)
	dst.Header.Set("X-Actor-Session", "stale")
	ForwardActor(dst, browser)
	if dst.Header.Get("X-Actor-Session") != "" || dst.Header.Get("X-Actor-Token") != "" || dst.Header.Get("X-Actor-User") != "" {
		t.Fatal("forged actor forwarded")
	}
	internal := httptest.NewRequest("POST", "https://internal.test/internal/v2/action", nil)
	internal.Header.Set("X-Actor-Session", "trusted-session")
	internal.Header.Set("X-Service-Token", "caller")
	ForwardActor(dst, internal)
	if dst.Header.Get("X-Actor-Session") != "trusted-session" {
		t.Fatal("authenticated internal forwarding lost")
	}
}

func TestCallBoundsResponseAndDoesNotFollowRedirects(t *testing.T) {
	redirected := false
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/redirect":
			http.Redirect(w, r, "/secret", http.StatusFound)
		case "/secret":
			redirected = true
		case "/large":
			_, _ = io.WriteString(w, strings.Repeat("x", (1<<20)+1))
		case "/get":
			if r.Header.Get("Content-Type") != "" {
				t.Error("empty GET has content type")
			}
			w.WriteHeader(204)
		}
	}))
	defer server.Close()
	client := &Client{HTTP: server.Client(), Config: Config{Token: "secret"}}
	status, _, err := client.Call(context.Background(), "GET", server.URL+"/redirect", nil, nil)
	if err != nil || status != 302 || redirected {
		t.Fatalf("redirect followed: status=%d error=%v", status, err)
	}
	if _, _, err = client.Call(context.Background(), "GET", server.URL+"/large", nil, nil); err == nil {
		t.Fatal("oversized response accepted")
	}
	if status, _, err = client.Call(context.Background(), "GET", server.URL+"/get", nil, nil); err != nil || status != 204 {
		t.Fatal("bodyless request failed")
	}
	if _, _, err = client.Call(context.Background(), "GET", "http://public.test/", nil, nil); err == nil {
		t.Fatal("plaintext dependency accepted")
	}
}

func TestAuthenticateForwardsDoubleSubmitCookiesAndStillDeniesInvalidCSRFOrOrigin(t *testing.T) {
	const decision = `{"active":true,"user_id":"019a1234-1234-7123-8123-123456789abc","session_id":"019a1234-1234-7123-8123-123456789abd","email":"owner@example.test","roles":["owner"],"permissions":["referrals:rates:write"],"mfa_verified":true,"is_verified":true}`
	identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var admission struct {
			Session string `json:"session_cookie"`
			CSRF    string `json:"csrf_token"`
			Method  string `json:"method"`
			Origin  string `json:"origin"`
		}
		if json.NewDecoder(r.Body).Decode(&admission) != nil {
			w.WriteHeader(400)
			return
		}
		session, sessionErr := r.Cookie("sso_session")
		csrf, csrfErr := r.Cookie("sso_csrf")
		if r.URL.Path != "/internal/v2/introspect" || r.Header.Get("X-Service-Token") != "workload" || r.Header.Get("X-Actor-Session") != "live-session" {
			w.WriteHeader(401)
			return
		}
		// This fixture implements the authoritative boundary's double-submit
		// policy, including the cookie that is not present in introspection DTOs.
		if sessionErr != nil || session.Value != admission.Session || admission.Session != "live-session" || csrfErr != nil || csrf.Value == "" || csrf.Value != admission.CSRF || r.Header.Get("X-CSRF-Token") != admission.CSRF || admission.Method != "PUT" || admission.Origin != "https://public.test" || r.Header.Get("Origin") != admission.Origin {
			w.WriteHeader(403)
			return
		}
		_, _ = io.WriteString(w, decision)
	}))
	defer identity.Close()
	client := &Client{HTTP: identity.Client(), Config: Config{Token: "workload", IdentityURL: identity.URL}}
	for _, tc := range []struct {
		name, cookie, header, origin string
		want                         int
	}{
		{"valid", "csrf-value", "csrf-value", "https://public.test", 200},
		{"missing_cookie", "", "csrf-value", "https://public.test", 403},
		{"mismatched_cookie", "other-value", "csrf-value", "https://public.test", 403},
		{"missing_header", "csrf-value", "", "https://public.test", 403},
		{"foreign_origin", "csrf-value", "csrf-value", "https://attacker.test", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("PUT", "https://public.test/api/v1/admin/referrals/rate", nil)
			r.AddCookie(&http.Cookie{Name: "sso_session", Value: "live-session"})
			if tc.cookie != "" {
				r.AddCookie(&http.Cookie{Name: "sso_csrf", Value: tc.cookie})
			}
			r.Header.Set("X-CSRF-Token", tc.header)
			r.Header.Set("Origin", tc.origin)
			r.Header.Set("X-Actor-Session", "forged-public-session")
			w := httptest.NewRecorder()
			actor, ok := client.Authenticate(w, r, "referrals:rates:write", true)
			if ok != (tc.want == 200) || w.Code != tc.want {
				t.Fatalf("admission ok=%v status=%d want=%d", ok, w.Code, tc.want)
			}
			if !ok && actor.UserID != "" {
				t.Fatal("denied request received actor")
			}
		})
	}
}
