// SPDX-License-Identifier: MIT
package audit

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"user-notification-service/internal/platform"
)

// Real PostgreSQL owns the journal. Only the separate identity HTTP boundary is
// replaced here; generic_identity.rs covers its live actor/operation policy.
func TestIntegrationSupportOwnCommandRecovery(t *testing.T) {
	for _, scenario := range []string{"committed-response-lost", "same-key-before-dispatch", "resume-before-dispatch"} {
		t.Run(scenario, func(t *testing.T) {
			db := auditDatabase(t)
			journal := &PostgresAudit{DB: db}
			command, _, err := journal.Admit(context.Background(), testActor, "revoke-sessions", testTarget, "support case", scenario)
			if err != nil {
				t.Fatal(err)
			}
			committed, posts := false, 0
			identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/internal/v2/introspect" {
					var input struct {
						Permission string `json:"permission"`
						Method     string `json:"method"`
						Origin     string `json:"origin"`
						CSRF       string `json:"csrf_token"`
					}
					if json.NewDecoder(r.Body).Decode(&input) != nil {
						t.Error("bad introspection")
						return
					}
					if input.Permission != "" && input.Permission != "users:sessions:revoke" {
						platform.Error(w, 403, "permission_denied")
						return
					}
					if input.Method == "POST" && (input.Origin != "https://app.test" || input.CSRF != "valid-csrf") {
						platform.Error(w, 403, "csrf_required")
						return
					}
					platform.JSON(w, 200, platform.Actor{Active: true, IsVerified: true, UserID: testActor, Email: "support@example.test", SessionID: testCommand, Roles: []string{"support"}, MFAVerified: true, Permissions: []string{"users:sessions:revoke"}})
					return
				}
				if r.Method == "GET" {
					if r.URL.Path != "/internal/v2/admin/commands/"+command.ID {
						t.Error("lookup changed command UUID")
					}
					if !committed {
						platform.Error(w, 404, "command_not_found")
						return
					}
					platform.JSON(w, 200, map[string]any{"command_id": command.ID, "status": "succeeded", "result": map[string]any{"revoked": 1}})
					return
				}
				posts++
				var input struct {
					Reason string `json:"reason"`
					Key    string `json:"idempotency_key"`
				}
				if json.NewDecoder(r.Body).Decode(&input) != nil || input.Key != command.ID || input.Reason != "support case" || r.URL.Path != "/internal/v2/admin/users/"+testTarget+"/sessions/revoke" {
					t.Error("resume changed persisted operation")
				}
				committed = true
				if scenario == "committed-response-lost" {
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					connection.Close()
					return
				}
				platform.JSON(w, 200, map[string]any{"revoked": 1})
			}))
			defer identity.Close()
			service := &Service{Config: platform.Config{IdentityURL: identity.URL}, Client: &platform.Client{Config: platform.Config{IdentityURL: identity.URL}, HTTP: identity.Client()}, Audit: journal}
			mux := http.NewServeMux()
			service.Routes(mux)
			request := func(method, path, body string, csrf bool) *httptest.ResponseRecorder {
				r := httptest.NewRequest(method, "https://app.test"+path, strings.NewReader(body))
				r.AddCookie(&http.Cookie{Name: "sso_session", Value: "session"})
				if csrf {
					r.Header.Set("Origin", "https://app.test")
					r.Header.Set("X-CSRF-Token", "valid-csrf")
				}
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, r)
				return w
			}
			statusPath := "/api/v1/admin/commands/" + command.ID
			// GET must not redispatch an admitted command, even with an authoritative 404.
			w := request("GET", statusPath, "", false)
			if w.Code != 200 || posts != 0 {
				t.Fatalf("support status denied or dispatched: %d %s posts=%d", w.Code, w.Body.String(), posts)
			}
			w = request("POST", statusPath+"/resume", "{}", false)
			if w.Code != 403 || posts != 0 {
				t.Fatalf("resume without CSRF: %d posts=%d", w.Code, posts)
			}
			if scenario == "committed-response-lost" {
				// The same-key public POST resumes the journal and loses the owner response.
				lost := request("POST", "/api/v1/admin/users/"+testTarget+"/revoke-sessions", `{"reason":"support case","idempotency_key":"`+scenario+`"}`, true)
				if lost.Code != 202 {
					t.Fatalf("lost response became terminal: %d %s", lost.Code, lost.Body.String())
				}
				stored, err := journal.Lookup(context.Background(), command.ID)
				if err != nil || stored.Status != "unknown" {
					t.Fatalf("lost response not journalled unknown: %+v %v", stored, err)
				}
				w = request("GET", statusPath, "", false)
			} else if scenario == "same-key-before-dispatch" {
				w = request("POST", "/api/v1/admin/users/"+testTarget+"/revoke-sessions", `{"reason":"support case","idempotency_key":"`+scenario+`"}`, true)
			} else {
				w = request("POST", statusPath+"/resume", "{}", true)
			}
			if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"succeeded"`) || posts != 1 {
				t.Fatalf("support recovery failed: %d %s posts=%d", w.Code, w.Body.String(), posts)
			}
			// Recreate repository and repeat both entrypoints: terminal state is durable, no redispatch.
			service.Audit = &PostgresAudit{DB: db}
			for _, path := range []string{statusPath, statusPath + "/resume"} {
				method, body := "GET", ""
				if strings.HasSuffix(path, "/resume") {
					method, body = "POST", "{}"
				}
				w = request(method, path, body, true)
				if w.Code != 200 || posts != 1 {
					t.Fatalf("terminal recovery repeated mutation: %d posts=%d", w.Code, posts)
				}
			}
			w = request("GET", "/api/v1/admin/audit", "", false)
			if w.Code != 403 {
				t.Fatalf("SUPPORT acquired global audit access: %d", w.Code)
			}
		})
	}
}

func TestSupportRecoveryDeniesForeignActorRevokedPermissionAndStaleMFA(t *testing.T) {
	for _, scenario := range []string{"foreign-actor", "revoked-permission", "stale-mfa", "wrong-original-permission", "unknown-action"} {
		for _, state := range []string{"requested", "succeeded"} {
			t.Run(scenario+"/"+state, func(t *testing.T) {
				command := Command{ID: testCommand, ActorID: testActor, Action: "revoke-sessions", TargetID: testTarget, Reason: "case", Status: state}
				actor := platform.Actor{Active: true, IsVerified: true, UserID: testActor, Email: "support@example.test", SessionID: testCommand, Roles: []string{"support"}, MFAVerified: true, Permissions: []string{"users:sessions:revoke"}}
				switch scenario {
				case "foreign-actor":
					actor.UserID = testTarget
				case "revoked-permission":
					actor.Permissions = []string{"users:read"}
				case "stale-mfa":
					actor.MFAVerified = false
				case "wrong-original-permission":
					command.Action = "ban"
				case "unknown-action":
					command.Action = "not-a-command"
				}
				identity := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/internal/v2/introspect" {
						platform.JSON(w, 200, actor)
						return
					}
					t.Error("unauthorized recovery reached owner")
					platform.Error(w, 404, "command_not_found")
				}))
				defer identity.Close()
				store := &commandStore{command: command}
				service := &Service{Config: platform.Config{IdentityURL: identity.URL}, Client: &platform.Client{Config: platform.Config{IdentityURL: identity.URL}, HTTP: identity.Client()}, Audit: store}
				mux := http.NewServeMux()
				service.Routes(mux)
				for _, method := range []string{"GET", "POST"} {
					path, body := "/api/v1/admin/commands/"+testCommand, ""
					if method == "POST" {
						path += "/resume"
						body = "{}"
					}
					w := adminRequest(mux, method, path, body)
					if w.Code != 403 || store.command.Status != state {
						t.Fatalf("unauthorized command exposed/recovered: %d %s", w.Code, w.Body.String())
					}
				}
			})
		}
	}
}

func TestRecoveryOwnerAuthorizationDenialDoesNotExposeJournal(t *testing.T) {
	for _, status := range []int{401, 403} {
		store := &commandStore{command: Command{ID: testCommand, ActorID: testActor, Action: "revoke-sessions", TargetID: testTarget, Reason: "case", Status: "unknown"}}
		mux := adminFixture(t, store, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				t.Error("denied owner lookup redispatched")
			}
			platform.Error(w, status, "forbidden")
		})
		for _, method := range []string{"GET", "POST"} {
			path, body := "/api/v1/admin/commands/"+testCommand, ""
			if method == "POST" {
				path += "/resume"
				body = "{}"
			}
			w := adminRequest(mux, method, path, body)
			if w.Code != status || strings.Contains(w.Body.String(), `"status":"unknown"`) || store.command.Status != "unknown" {
				t.Fatalf("owner authorization failure exposed/changed journal: %d %s", w.Code, w.Body.String())
			}
		}
	}
}
