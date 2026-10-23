package notification

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"platform-contracts/gen/go/internalhttpv2"
	"strings"
	"time"
)

type recipientPolicy = internalhttpv2.RecipientPolicy
type eligibility = internalhttpv2.UserEligibility

var errAuthorityUnavailable = errors.New("recipient_policy_unavailable")

func (s *Service) recipient(ctx context.Context, user string) (recipientPolicy, error) {
	var p recipientPolicy
	status, raw, err := s.Client.Call(ctx, "GET", s.Config.ProfileURL+"/internal/v2/users/"+url.PathEscape(user)+"/recipient-policy", nil, nil)
	if err != nil || status != 200 || json.Unmarshal(raw, &p) != nil || p.UserID != user || p.Email == "" {
		return p, errAuthorityUnavailable
	}
	return p, nil
}
func (s *Service) eligibility(ctx context.Context, user string) (eligibility, error) {
	var e eligibility
	status, raw, err := s.Client.Call(ctx, "GET", s.Config.IdentityURL+"/internal/v2/users/"+url.PathEscape(user)+"/eligibility", nil, nil)
	if err != nil || status != 200 || json.Unmarshal(raw, &e) != nil || e.UserID != user {
		return e, errAuthorityUnavailable
	}
	return e, nil
}
func policyAllows(channel string, b Body, p recipientPolicy, e eligibility) bool {
	if !e.Active || e.Banned {
		return false
	}
	if channel == "email" {
		if b.Category == "transactional" {
			return b.To == e.Email
		}
		if b.To != p.Email || b.To != e.Email {
			return false
		}
		switch b.Category {
		case "security":
			return p.EmailSecurity
		case "login":
			return p.EmailLoginAlerts
		case "subscription":
			return p.EmailSubscription
		}
		return false
	}
	if channel != "telegram" || p.TelegramChatID == nil || *p.TelegramChatID != b.To || p.BindingVersion != b.BindingVersion {
		return false
	}
	switch b.Category {
	case "admin":
		if !e.Staff || b.RequiredPermission == "" {
			return false
		}
		for _, perm := range e.Permissions {
			if perm == b.RequiredPermission {
				return true
			}
		}
		return false
	case "security":
		return p.TelegramSecurity
	case "login":
		return p.TelegramLoginAlerts
	case "subscription":
		return p.TelegramSubscription
	case "trading":
		return p.TelegramTrading
	}
	return false
}
func (s *Service) checkAttempt(ctx context.Context, j Job, b Body) (bool, string, error) {
	user := b.UserID
	if user == "" {
		user = j.UserID
	}
	if user == "" {
		return true, "invalid_recipient", errors.New("recipient absent")
	}
	if j.Channel == "telegram" && b.Category == "" {
		return true, "binding_snapshot_missing", errors.New("legacy binding cannot be proven")
	}
	e, err := s.eligibility(ctx, user)
	if err != nil {
		return false, "recipient_policy_unavailable", err
	}
	p, err := s.recipient(ctx, user)
	if err != nil {
		return false, "recipient_policy_unavailable", err
	}
	if b.Category == "" {
		b.EventType = j.EventType
		b.FlowEventID = j.EventID
		b.Category = eventCategory(j.EventType)
	}
	if b.Category == "transactional" {
		if !e.Active || e.Banned || b.To != e.Email {
			return true, "recipient_ineligible", errors.New("recipient ineligible")
		}
		query := url.Values{"event_id": {b.FlowEventID}, "type": {b.EventType}, "target": {b.To}}
		status, raw, err := s.Client.Call(ctx, "GET", s.Config.IdentityURL+"/internal/v2/users/"+url.PathEscape(user)+"/notification-flow?"+query.Encode(), nil, nil)
		var flow internalhttpv2.NotificationFlow
		if err != nil || status != 200 || json.Unmarshal(raw, &flow) != nil {
			return false, "flow_unavailable", errAuthorityUnavailable
		}
		if !flow.Active {
			return true, "flow_expired", errors.New("flow expired")
		}
		return false, "", nil
	}
	if !policyAllows(j.Channel, b, p, e) {
		return true, "recipient_ineligible", errors.New("recipient ineligible")
	}
	if b.SubscriptionID != "" && (b.Category == "trading" || b.EventType == "subscription.expiry_reminder" || b.EventType == "external.readiness_changed") {
		current, err := s.subscription(ctx, b.SubscriptionID)
		if err != nil {
			return false, "subscription_unavailable", err
		}
		if current.UserID != user {
			return true, "subscription_ineligible", errors.New("subscription ownership changed")
		}
		if b.EventType != "external.readiness_changed" && (strings.ToUpper(current.State) != "ACTIVE" || !time.Now().Before(current.PeriodEnd)) {
			return true, "subscription_ineligible", errors.New("subscription ineligible")
		}
		if b.Category == "trading" && !current.Allowed {
			return true, "subscription_ineligible", errors.New("entitlement denied")
		}
		if b.EventType == "subscription.expiry_reminder" && (current.Version != b.SubscriptionVersion || b.PeriodEnd == nil || !current.PeriodEnd.Equal(*b.PeriodEnd)) {
			return true, "reminder_stale", errors.New("stale reminder")
		}
	}
	return false, "", nil
}
func eventCategory(event string) string {
	if event == "notification.email_verification" || event == "notification.password_reset" {
		return "transactional"
	}
	if event == "identity.user_logged_in" {
		return "login"
	}
	if strings.HasPrefix(event, "identity.") {
		return "security"
	}
	if event == "external.trading_alert" {
		return "trading"
	}
	return "subscription"
}
