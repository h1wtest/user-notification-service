// SPDX-License-Identifier: MIT
package audit

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"user-notification-service/internal/platform"
)

const testActor = "019a0000-0000-7000-8000-000000000002"
const testTarget = "019a0000-0000-7000-8000-000000000003"
const testCommand = "019a0000-0000-7000-8000-000000000005"

// This double replaces only durable storage; authorization and owner HTTP
// serialization, outcome classification and recovery are the real service.
type commandStore struct {
	Repository
	command  Command
	admitted int
}

func (p *commandStore) Admit(_ context.Context, actor, action, target, reason, key string) (Command, bool, error) {
	if p.command.ID != "" {
		if p.command.ActorID != actor || p.command.Action != action || p.command.TargetID != target || p.command.Reason != reason || p.command.IdempotencyKey != key {
			return Command{}, false, ErrIdempotencyConflict
		}
		return p.command, false, nil
	}
	p.admitted++
	p.command = Command{ID: testCommand, ActorID: actor, Action: action, TargetID: target, Reason: reason, IdempotencyKey: key, Status: "requested", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	return p.command, true, nil
}
func (p *commandStore) Lookup(_ context.Context, id string) (Command, error) {
	if id != p.command.ID {
		return Command{}, ErrCommandNotFound
	}
	return p.command, nil
}
func (p *commandStore) Finish(_ context.Context, id, status string, result json.RawMessage) error {
	if id != p.command.ID {
		return ErrCommandNotFound
	}
	p.command.Status = status
	p.command.Result = result
	return nil
}

func adminFixture(t *testing.T, store *commandStore, owner http.HandlerFunc) *http.ServeMux {
	t.Helper()
	identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/v2/introspect" {
			platform.JSON(w, 200, platform.Actor{Active: true, IsVerified: true, UserID: testActor, Email: "admin@example.test", SessionID: testCommand, Roles: []string{"ADMIN"}, MFAVerified: true, Permissions: []string{"users:read", "users:write", "users:sessions:revoke", "audit:read", "notifications:read", "notifications:replay"}})
			return
		}
		owner(w, r)
	}))
	t.Cleanup(identity.Close)
	s := &Service{Config: platform.Config{IdentityURL: identity.URL, NotificationURL: identity.URL}, Client: &platform.Client{Config: platform.Config{IdentityURL: identity.URL}, HTTP: identity.Client()}, Audit: store}
	m := http.NewServeMux()
	s.Routes(m)
	return m
}
func adminRequest(m *http.ServeMux, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "https://app.test"+path, strings.NewReader(body))
	r.AddCookie(&http.Cookie{Name: "sso_session", Value: "session"})
	w := httptest.NewRecorder()
	m.ServeHTTP(w, r)
	return w
}

func TestEveryModerationCommandRequiresReason(t *testing.T) {
	for _, action := range []string{"ban", "unban", "revoke-sessions"} {
		t.Run(action, func(t *testing.T) {
			p := &commandStore{}
			m := adminFixture(t, p, func(w http.ResponseWriter, r *http.Request) { t.Fatal("invalid command reached identity owner") })
			w := adminRequest(m, "POST", "/api/v1/admin/users/"+testTarget+"/"+action, `{"reason":"   ","idempotency_key":"one"}`)
			if w.Code != 400 || p.admitted != 0 {
				t.Fatalf("reason was admitted: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestMalformedOwnerSuccessRemainsUnknownAndSameIDRecovers(t *testing.T) {
	p := &commandStore{}
	var posts int
	m := adminFixture(t, p, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			posts++
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"unexpected":"not a command result"}`))
			return
		}
		if r.URL.Path != "/internal/v2/admin/commands/"+testCommand {
			t.Fatalf("recovery used wrong identity: %s", r.URL.Path)
		}
		platform.JSON(w, 200, map[string]any{"command_id": testCommand, "status": "succeeded", "result": map[string]any{"user_id": testTarget, "is_banned": true}})
	})
	body := `{"reason":"abuse investigation","idempotency_key":"case-one"}`
	w := adminRequest(m, "POST", "/api/v1/admin/users/"+testTarget+"/ban", body)
	if w.Code != 202 || p.command.Status != "unknown" || !strings.Contains(w.Body.String(), testCommand) {
		t.Fatalf("malformed response reported success/lost ID: %d %s %+v", w.Code, w.Body.String(), p.command)
	}
	w = adminRequest(m, "POST", "/api/v1/admin/users/"+testTarget+"/ban", body)
	if posts != 1 || p.admitted != 1 || !strings.Contains(w.Body.String(), testCommand) {
		t.Fatalf("retry created another owner command: posts=%d admits=%d %s", posts, p.admitted, w.Body.String())
	}
	w = adminRequest(m, "GET", "/api/v1/admin/commands/"+testCommand, "")
	if w.Code != 200 || p.command.Status != "succeeded" || !strings.Contains(w.Body.String(), `"status":"succeeded"`) {
		t.Fatalf("same-ID recovery failed: %d %s", w.Code, w.Body.String())
	}
}

func TestChangedIdempotentCommandConflictsBeforeOwner(t *testing.T) {
	p := &commandStore{command: Command{ID: testCommand, ActorID: testActor, Action: "ban", TargetID: testTarget, Reason: "original", IdempotencyKey: "same", Status: "succeeded"}}
	m := adminFixture(t, p, func(w http.ResponseWriter, r *http.Request) { t.Fatal("conflicting command reached owner") })
	w := adminRequest(m, "POST", "/api/v1/admin/users/"+testTarget+"/ban", `{"reason":"changed","idempotency_key":"same"}`)
	if w.Code != 409 {
		t.Fatalf("conflict=%d %s", w.Code, w.Body.String())
	}
}

func TestCommandRecoveryNeverAcceptsDifferentOwnerCommand(t *testing.T) {
	p := &commandStore{command: Command{ID: testCommand, ActorID: testActor, Action: "ban", TargetID: testTarget, Reason: "case", IdempotencyKey: "one", Status: "unknown"}}
	m := adminFixture(t, p, func(w http.ResponseWriter, r *http.Request) {
		platform.JSON(w, 200, map[string]any{"command_id": testTarget, "status": "succeeded"})
	})
	w := adminRequest(m, "GET", "/api/v1/admin/commands/"+testCommand, "")
	if w.Code != 200 || p.command.Status != "unknown" || !strings.Contains(w.Body.String(), `"status":"unknown"`) {
		t.Fatalf("foreign result became success: %d %s", w.Code, w.Body.String())
	}
}

func TestPaginationRejectsMalformedAndOversizedInput(t *testing.T) {
	for _, q := range []string{"limit=0", "limit=101", "limit=garbage", "cursor=not-a-cursor"} {
		r := httptest.NewRequest("GET", "https://app.test/?"+q, nil)
		if _, err := parsePage(r); err == nil {
			t.Fatalf("unsafe page accepted: %s", q)
		}
	}
	r := httptest.NewRequest("GET", "https://app.test/", nil)
	page, err := parsePage(r)
	if err != nil || page.Limit != 50 {
		t.Fatalf("default page: %+v %v", page, err)
	}
	original := Page{Before: time.Date(2026, 10, 24, 12, 0, 0, 123, time.UTC), BeforeID: testCommand}
	r = httptest.NewRequest("GET", "https://app.test/?cursor="+encodeCursor(original.Before, original.BeforeID), nil)
	got, err := parsePage(r)
	if err != nil || !got.Before.Equal(original.Before) || got.BeforeID != testCommand {
		t.Fatalf("cursor lost stable tuple: %+v %v", got, err)
	}
}

func TestOwnerDenialIsNotUnknown(t *testing.T) {
	p := &commandStore{}
	m := adminFixture(t, p, func(w http.ResponseWriter, r *http.Request) { platform.Error(w, 403, "permission_denied") })
	w := adminRequest(m, "POST", "/api/v1/admin/users/"+testTarget+"/unban", `{"reason":"case","idempotency_key":"denial"}`)
	if w.Code != 200 || p.command.Status != "denied" {
		t.Fatalf("denial not journaled: %d %s", w.Code, w.Body.String())
	}
}

func TestIdentityUnavailableReturnsRecoverableUnknown(t *testing.T) {
	p := &commandStore{}
	m := adminFixture(t, p, func(w http.ResponseWriter, r *http.Request) { platform.Error(w, 503, "identity_unavailable") })
	w := adminRequest(m, "POST", "/api/v1/admin/users/"+testTarget+"/ban", `{"reason":"case","idempotency_key":"timeout"}`)
	if w.Code != 202 || p.command.Status != "unknown" || !strings.Contains(w.Body.String(), testCommand) {
		t.Fatalf("dependency failure lost command: %d %s", w.Code, w.Body.String())
	}
}

func TestUnsafeIdentityResponseNeverPassesThrough(t *testing.T) {
	p := &commandStore{}
	m := adminFixture(t, p, func(w http.ResponseWriter, r *http.Request) {
		platform.JSON(w, 200, map[string]any{"id": testTarget, "email": "user@example.test", "is_verified": true, "is_banned": false, "ban_reason": nil, "created_at": "2026-10-24T00:00:00Z", "mfa_enabled": false, "roles": []string{"USER"}, "password_hash": "private-secret"})
	})
	w := adminRequest(m, "GET", "/api/v1/admin/users/"+testTarget, "")
	if w.Code != 503 || strings.Contains(w.Body.String(), "private-secret") {
		t.Fatalf("unsafe identity detail leaked: %d %s", w.Code, w.Body.String())
	}
}

func TestMalformedOwnerDenialRemainsUnknown(t *testing.T) {
	p := &commandStore{}
	m := adminFixture(t, p, func(w http.ResponseWriter, r *http.Request) {
		platform.JSON(w, 403, map[string]string{"provider_secret": "not a valid denial"})
	})
	w := adminRequest(m, "POST", "/api/v1/admin/users/"+testTarget+"/ban", `{"reason":"case","idempotency_key":"malformed-denial"}`)
	if w.Code != 202 || p.command.Status != "unknown" || strings.Contains(w.Body.String(), "provider_secret") {
		t.Fatalf("malformed denial treated terminal: %d %s", w.Code, w.Body.String())
	}
}

func TestDeliveryReplayPreservesSafeOwnerConflictAndReason(t *testing.T) {
	p := &commandStore{}
	m := adminFixture(t, p, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/deliveries/"+testTarget+"/replay" {
			t.Fatalf("wrong replay owner: %s", r.URL.Path)
		}
		var body map[string]any
		if json.NewDecoder(r.Body).Decode(&body) != nil || body["reason"] != "incident case" {
			t.Error("replay reason was dropped")
		}
		platform.Error(w, 409, "delivery_not_failed")
	})
	w := adminRequest(m, "POST", "/api/v1/admin/deliveries/"+testTarget+"/replay", `{"reason":"incident case"}`)
	if w.Code != 409 || !strings.Contains(w.Body.String(), "delivery_not_failed") {
		t.Fatalf("failed-only denial was hidden: %d %s", w.Code, w.Body.String())
	}
}

func TestLostOwnerResponseRecoversCommittedSameCommand(t *testing.T) {
	p := &commandStore{}
	var committed string
	m := adminFixture(t, p, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			var body struct {
				Reason string `json:"reason"`
				Key    string `json:"idempotency_key"`
			}
			if json.NewDecoder(r.Body).Decode(&body) != nil || body.Key != testCommand {
				t.Error("owner did not receive persisted journal UUID")
			}
			committed = body.Key
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			connection.Close()
			return
		}
		if committed != testCommand || r.URL.Path != "/internal/v2/admin/commands/"+committed {
			t.Fatal("recovery changed command identity")
		}
		platform.JSON(w, 200, map[string]any{"command_id": committed, "status": "succeeded", "result": map[string]any{"user_id": testTarget, "is_banned": true}})
	})
	w := adminRequest(m, "POST", "/api/v1/admin/users/"+testTarget+"/ban", `{"reason":"case","idempotency_key":"lost-response"}`)
	if w.Code != 202 || p.command.Status != "unknown" || !strings.Contains(w.Body.String(), testCommand) {
		t.Fatalf("lost response reported success: %d %s", w.Code, w.Body.String())
	}
	w = adminRequest(m, "GET", "/api/v1/admin/commands/"+testCommand, "")
	if w.Code != 200 || p.command.Status != "succeeded" {
		t.Fatalf("committed owner outcome did not recover: %d %s", w.Code, w.Body.String())
	}
}

func TestAdmittedUndispatchedCommandResumesSameID(t *testing.T) {
	for _, scenario := range []struct{ method, status string }{{"POST", "requested"}, {"POST", "unknown"}} {
		t.Run(scenario.method+"_"+scenario.status, func(t *testing.T) {
			method := scenario.method
			p := &commandStore{command: Command{ID: testCommand, ActorID: testActor, Action: "ban", TargetID: testTarget, Reason: "stored reason", IdempotencyKey: "resume", Status: scenario.status}}
			posts := 0
			m := adminFixture(t, p, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "GET" {
					platform.Error(w, 404, "command_not_found")
					return
				}
				posts++
				var body struct {
					Reason string `json:"reason"`
					Key    string `json:"idempotency_key"`
				}
				if json.NewDecoder(r.Body).Decode(&body) != nil || body.Key != testCommand || body.Reason != "stored reason" || r.URL.Path != "/internal/v2/admin/users/"+testTarget+"/ban" {
					t.Error("resumption changed immutable command")
				}
				platform.JSON(w, 200, map[string]any{"user_id": testTarget, "is_banned": true})
			})
			path, body := "/api/v1/admin/commands/"+testCommand, ""
			if method == "POST" {
				path = "/api/v1/admin/users/" + testTarget + "/ban"
				body = `{"reason":"stored reason","idempotency_key":"resume"}`
			}
			w := adminRequest(m, method, path, body)
			if w.Code != 200 || p.command.Status != "succeeded" || posts != 1 || p.admitted != 0 {
				t.Fatalf("admitted command stranded/replaced: %d %s posts=%d admits=%d", w.Code, w.Body.String(), posts, p.admitted)
			}
		})
	}
}

func TestRecoveryCannotBlindlyResubmitUncertainOwnerStatus(t *testing.T) {
	for _, status := range []int{503, 404} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			p := &commandStore{command: Command{ID: testCommand, ActorID: testActor, Action: "ban", TargetID: testTarget, Reason: "case", IdempotencyKey: "one", Status: "unknown"}}
			m := adminFixture(t, p, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					t.Error("uncertain/malformed owner lookup caused dispatch")
				}
				platform.JSON(w, status, map[string]string{"untrusted": "not authoritative absence"})
			})
			w := adminRequest(m, "POST", "/api/v1/admin/commands/"+testCommand+"/resume", "{}")
			if w.Code != 202 || p.command.Status != "unknown" {
				t.Fatalf("uncertain lookup became terminal: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestRecoveryCannotResubmitAnotherActorsCommand(t *testing.T) {
	p := &commandStore{command: Command{ID: testCommand, ActorID: testTarget, Action: "ban", TargetID: testTarget, Reason: "case", IdempotencyKey: "one", Status: "requested"}}
	m := adminFixture(t, p, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			t.Error("foreign actor resubmitted command")
		}
		platform.Error(w, 404, "command_not_found")
	})
	w := adminRequest(m, "POST", "/api/v1/admin/commands/"+testCommand+"/resume", "{}")
	if p.command.Status != "requested" || strings.Contains(w.Body.String(), `"status":"succeeded"`) {
		t.Fatalf("foreign command recovered: %d %s", w.Code, w.Body.String())
	}
}

func TestRecoveryRechecksMutationPermissionBeforeResubmit(t *testing.T) {
	p := &commandStore{command: Command{ID: testCommand, ActorID: testActor, Action: "revoke-sessions", TargetID: testTarget, Reason: "case", IdempotencyKey: "one", Status: "requested"}}
	identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/v2/introspect" {
			var input struct {
				Permission string `json:"permission"`
			}
			if json.NewDecoder(r.Body).Decode(&input) != nil {
				t.Error("invalid introspect request")
			}
			if input.Permission == "users:sessions:revoke" {
				platform.Error(w, 403, "permission_denied")
				return
			}
			platform.JSON(w, 200, platform.Actor{Active: true, IsVerified: true, UserID: testActor, Email: "admin@example.test", SessionID: testCommand, Roles: []string{"SUPPORT"}, MFAVerified: true, Permissions: []string{"audit:read"}})
			return
		}
		if r.Method == "POST" {
			t.Error("revoked permission still dispatched")
		}
		platform.Error(w, 404, "command_not_found")
	}))
	defer identity.Close()
	s := &Service{Config: platform.Config{IdentityURL: identity.URL}, Client: &platform.Client{Config: platform.Config{IdentityURL: identity.URL}, HTTP: identity.Client()}, Audit: p}
	m := http.NewServeMux()
	s.Routes(m)
	w := adminRequest(m, "POST", "/api/v1/admin/commands/"+testCommand+"/resume", "{}")
	if w.Code != 403 || p.command.Status != "requested" {
		t.Fatalf("revoked permission recovery: %d %s", w.Code, w.Body.String())
	}
}

func TestDeliveryProxyPreservesOnlyPaginationHeader(t *testing.T) {
	p := &commandStore{}
	m := adminFixture(t, p, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Next-Cursor", "opaque-tuple-cursor")
		w.Header().Set("Set-Cookie", "private-owner-cookie=secret")
		w.Header().Set("X-Provider-Secret", "must-not-pass")
		platform.JSON(w, 200, []any{})
	})
	w := adminRequest(m, "GET", "/api/v1/admin/deliveries?limit=1", "")
	if w.Code != 200 || w.Header().Get("X-Next-Cursor") != "opaque-tuple-cursor" {
		t.Fatalf("delivery next cursor lost: %d %+v", w.Code, w.Header())
	}
	if w.Header().Get("Set-Cookie") != "" || w.Header().Get("X-Provider-Secret") != "" {
		t.Fatalf("unallowlisted owner header escaped: %+v", w.Header())
	}
}

func TestStatusReadNeverDispatchesAndExplicitResumePreservesCSRF(t *testing.T) {
	p := &commandStore{command: Command{ID: testCommand, ActorID: testActor, Action: "ban", TargetID: testTarget, Reason: "stored case", IdempotencyKey: "one", Status: "requested"}}
	posts := 0
	identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/v2/introspect" {
			var input struct {
				Method string `json:"method"`
				Origin string `json:"origin"`
				CSRF   string `json:"csrf_token"`
			}
			if json.NewDecoder(r.Body).Decode(&input) != nil {
				t.Error("bad introspection body")
			}
			if input.Method != "GET" && (input.Origin != "https://app.test" || input.CSRF != "valid-csrf") {
				platform.Error(w, 403, "csrf_required")
				return
			}
			platform.JSON(w, 200, platform.Actor{Active: true, IsVerified: true, UserID: testActor, Email: "admin@example.test", SessionID: testCommand, Roles: []string{"ADMIN"}, MFAVerified: true, Permissions: []string{"audit:read", "users:write"}})
			return
		}
		if r.Method == "GET" {
			platform.Error(w, 404, "command_not_found")
			return
		}
		posts++
		// Real identity mutation admission requires original browser Origin/CSRF,
		// even if a preceding GET status lookup was authorized.
		if r.Header.Get("Origin") != "https://app.test" || r.Header.Get("X-CSRF-Token") != "valid-csrf" {
			platform.Error(w, 403, "csrf_required")
			return
		}
		var input struct {
			Reason string `json:"reason"`
			Key    string `json:"idempotency_key"`
		}
		if json.NewDecoder(r.Body).Decode(&input) != nil || input.Key != testCommand || input.Reason != "stored case" {
			t.Error("resume changed journal operation")
		}
		platform.JSON(w, 200, map[string]any{"user_id": testTarget, "is_banned": true})
	}))
	defer identity.Close()
	s := &Service{Config: platform.Config{IdentityURL: identity.URL}, Client: &platform.Client{Config: platform.Config{IdentityURL: identity.URL}, HTTP: identity.Client()}, Audit: p}
	m := http.NewServeMux()
	s.Routes(m)
	w := adminRequest(m, "GET", "/api/v1/admin/commands/"+testCommand, "")
	if w.Code != 200 || posts != 0 || p.command.Status != "requested" {
		t.Fatalf("GET status performed mutation: %d posts=%d status=%s", w.Code, posts, p.command.Status)
	}
	w = adminRequest(m, "POST", "/api/v1/admin/commands/"+testCommand+"/resume", "{}")
	if w.Code != 403 || posts != 0 || p.command.Status != "requested" {
		t.Fatalf("missing CSRF resume reached owner: %d posts=%d", w.Code, posts)
	}
	r := httptest.NewRequest("POST", "https://app.test/api/v1/admin/commands/"+testCommand+"/resume", strings.NewReader("{}"))
	r.AddCookie(&http.Cookie{Name: "sso_session", Value: "session"})
	r.Header.Set("Origin", "https://app.test")
	r.Header.Set("X-CSRF-Token", "valid-csrf")
	w = httptest.NewRecorder()
	m.ServeHTTP(w, r)
	if w.Code != 200 || posts != 1 || p.command.Status != "succeeded" {
		t.Fatalf("valid explicit resume failed: %d %s posts=%d", w.Code, w.Body.String(), posts)
	}
}
