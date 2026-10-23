package profile

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"user-notification-service/internal/platform"
)

func TestOverviewUnavailableSectionDoesNotBecomeEmptySuccess(t *testing.T) {
	owner := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/orders" {
			http.Error(w, "unavailable", 503)
			return
		}
		if r.URL.Path == "/api/v1/auth/me" {
			w.Write([]byte(`{"user":{"id":"019a0000-0000-7000-8000-000000000001","email":"a@example.invalid","is_verified":true,"mfa_enabled":false,"mfa_verified":false,"roles":[],"permissions":[]},"session_id":"019a0000-0000-7000-8000-000000000002","mfa_verified":false,"csrf_token":"must-not-compose"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"items":[],"next_cursor":null}`))
	}))
	defer owner.Close()
	s := &Service{Config: platform.Config{IdentityURL: owner.URL, BillingURL: owner.URL, SubscriptionURL: owner.URL, ReferralURL: owner.URL}, Client: &platform.Client{HTTP: owner.Client()}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := s.composeOverview(ctx, httptest.NewRequest("GET", "https://app.invalid/api/v1/users/me/overview", nil))
	if result.Orders.Status != "unavailable" || result.Orders.Data != nil {
		t.Fatalf("orders failure hidden: %+v", result.Orders)
	}
	if result.Subscriptions.Status != "available" {
		t.Fatalf("healthy section lost: %+v", result.Subscriptions)
	}
	raw, err := json.Marshal(result.Account.Data)
	if err != nil {
		t.Fatal(err)
	}
	var account map[string]json.RawMessage
	if err := json.Unmarshal(raw, &account); err != nil {
		t.Fatal(err)
	}
	if _, ok := account["csrf_token"]; ok {
		t.Fatal("overview forwarded identity CSRF token")
	}
}
