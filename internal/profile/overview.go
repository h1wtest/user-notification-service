package profile

import (
	"context"
	"encoding/json"
	"net/http"
	"platform-contracts/gen/go/businessv1"
	"sync"
	"time"
	"user-notification-service/internal/platform"
)

func (s *Service) overview(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.Client.Authenticate(w, r, "", false); !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	platform.JSON(w, 200, s.composeOverview(ctx, r))
}
func (s *Service) composeOverview(ctx context.Context, r *http.Request) businessv1.Overview {
	result := businessv1.Overview{Account: businessv1.AccountSection{Status: "unavailable"}, Subscriptions: businessv1.SubscriptionsSection{Status: "unavailable"}, Orders: businessv1.OrdersSection{Status: "unavailable"}, Referrals: businessv1.ReferralsSection{Status: "unavailable"}}
	var wg sync.WaitGroup
	fetch := func(target string, apply func([]byte) bool) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			status, raw, err := s.Client.Call(bounded, http.MethodGet, target, nil, r)
			if err != nil || status != 200 || !json.Valid(raw) {
				return
			}
			apply(raw)
		}()
	}
	fetch(s.Config.IdentityURL+"/api/v1/auth/me", func(raw []byte) bool {
		// Identity's response includes CSRF. Select only the safe generated summary;
		// the closed contract itself never admits session secrets into overview.
		var safe struct {
			User        businessv1.UserAccount `json:"user"`
			SessionID   string                 `json:"session_id"`
			MFAVerified bool                   `json:"mfa_verified"`
		}
		if json.Unmarshal(raw, &safe) != nil {
			return false
		}
		value := businessv1.AccountSummary{User: safe.User, SessionID: safe.SessionID, MFAVerified: safe.MFAVerified}
		if value.Validate() != nil {
			return false
		}
		result.Account = businessv1.AccountSection{Status: "available", Data: &value}
		return true
	})
	fetch(s.Config.SubscriptionURL+"/api/v1/subscriptions?limit=5", func(raw []byte) bool {
		var value businessv1.SubscriptionsResponse
		if json.Unmarshal(raw, &value) != nil {
			return false
		}
		result.Subscriptions = businessv1.SubscriptionsSection{Status: "available", Data: &value}
		return true
	})
	fetch(s.Config.BillingURL+"/api/v1/orders?limit=5", func(raw []byte) bool {
		var value businessv1.OrdersResponse
		if json.Unmarshal(raw, &value) != nil {
			return false
		}
		result.Orders = businessv1.OrdersSection{Status: "available", Data: &value}
		return true
	})
	fetch(s.Config.ReferralURL+"/api/v1/referrals", func(raw []byte) bool {
		var value businessv1.ReferralSummary
		if json.Unmarshal(raw, &value) != nil {
			return false
		}
		result.Referrals = businessv1.ReferralsSection{Status: "available", Data: &value}
		return true
	})
	wg.Wait()
	return result
}
