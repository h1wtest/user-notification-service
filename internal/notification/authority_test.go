package notification

import (
	"context"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"testing"
	"user-notification-service/internal/platform"
)

func TestEveryAttemptRechecksFlowAndUnknownPolicyDefers(t *testing.T) {
	user := uuid.NewString()
	event := uuid.NewString()
	flowActive := true
	unavailable := false
	flowCalls := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/v2/users/"+user+"/eligibility" {
			platform.JSON(w, 200, map[string]any{"user_id": user, "email": "user@example.test", "active": true, "verified": false, "banned": false, "locale": "en", "staff": false, "permissions": []string{}})
			return
		}
		if r.URL.Path == "/internal/v2/users/"+user+"/recipient-policy" {
			if unavailable {
				w.WriteHeader(503)
				return
			}
			platform.JSON(w, 200, map[string]any{"user_id": user, "email": "user@example.test", "locale": "en", "telegram_chat_id": nil, "binding_version": 0, "email_subscription": false, "email_security": false, "telegram_subscription": false, "telegram_security": false, "telegram_trading": false, "email_login_alerts": false, "telegram_login_alerts": false})
			return
		}
		if r.URL.Path == "/internal/v2/users/"+user+"/notification-flow" {
			flowCalls++
			if r.URL.Query().Get("event_id") != event || r.URL.Query().Get("target") != "user@example.test" {
				t.Error("flow not bound")
			}
			platform.JSON(w, 200, map[string]bool{"active": flowActive})
			return
		}
		w.WriteHeader(404)
	}))
	defer server.Close()
	c := platform.Config{IdentityURL: server.URL, ProfileURL: server.URL}
	s := New(nil, c, &platform.Client{Config: c, HTTP: server.Client()})
	job := Job{ID: uuid.NewString(), EventID: event, UserID: user, Channel: "email"}
	body := Body{To: "user@example.test", UserID: user, Category: "transactional", EventType: "notification.email_verification", FlowEventID: event}
	if _, _, err := s.checkAttempt(context.Background(), job, body); err != nil {
		t.Fatalf("nondisablable flow rejected: %v", err)
	}
	flowActive = false
	if permanent, reason, err := s.checkAttempt(context.Background(), job, body); err == nil || !permanent || reason != "flow_expired" {
		t.Fatal("consumed flow sent")
	}
	if flowCalls != 2 {
		t.Fatal("flow decision cached")
	}
	unavailable = true
	body.Category = "subscription"
	if permanent, _, err := s.checkAttempt(context.Background(), job, body); err == nil || permanent {
		t.Fatal("unknown policy did not defer")
	}
}
