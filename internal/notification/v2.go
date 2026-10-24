package notification

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"net/url"
	"platform-contracts/gen/go/eventsv2"
	"platform-contracts/gen/go/internalhttpv2"
	"strings"
	"time"
	"user-notification-service/internal/platform"
)

type deliveryPlan struct {
	UserID, Locale, Target, Category, RequiredPermission, SubscriptionID string
	Version                                                              int64
	PeriodEnd, ExpiresAt                                                 *time.Time
	Vars                                                                 map[string]string
	Reminder                                                             *reminderPlan
}

var errSequenceGap = errors.New("notification_event_sequence_gap")

func (s *Service) ApplyV2(ctx context.Context, e eventsv2.Envelope) error {
	if err := e.Validate(); err != nil {
		return platform.ErrPoison
	}
	raw := e.Payload
	if e.EncryptedPayload != "" {
		var err error
		raw, err = platform.Decrypt(s.Config.DeliveryKey, e.EventID, e.EncryptedPayload)
		if err != nil {
			return platform.ErrPoison
		}
	}
	if err := eventsv2.ValidatePayload(e.EventType, raw); err != nil {
		return platform.ErrPoison
	}
	p := deliveryPlan{Category: eventCategory(e.EventType), Vars: map[string]string{"occurred_at": e.OccurredAt.UTC().Format(time.RFC3339)}}
	switch e.EventType {
	case "notification.email_verification":
		var v eventsv2.EmailVerification
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID, p.Target, p.ExpiresAt = v.UserID, v.Email, &v.ExpiresAt
		p.Vars["code"] = v.Code
		p.Vars["verification_url"] = strings.TrimRight(s.Config.Origin, "/") + "/?user_id=" + url.QueryEscape(v.UserID)
		p.Vars["expires_at"] = v.ExpiresAt.UTC().Format(time.RFC3339)
		if v.Locale != nil {
			p.Locale = *v.Locale
		}
	case "notification.password_reset":
		var v eventsv2.PasswordReset
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID, p.Target, p.ExpiresAt = v.UserID, v.Email, &v.ExpiresAt
		p.Vars["reset_url"] = strings.TrimRight(s.Config.Origin, "/") + "/?token=" + url.QueryEscape(v.Token)
		p.Vars["expires_at"] = v.ExpiresAt.UTC().Format(time.RFC3339)
		if v.Locale != nil {
			p.Locale = *v.Locale
		}
	case "identity.user_logged_in":
		var v eventsv2.UserLoggedIn
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID = v.UserID
		p.Vars["device_name"] = v.DeviceName
		p.Vars["ip_address"] = v.IPAddress
		p.Vars["occurred_at"] = v.LoggedInAt.UTC().Format(time.RFC3339)
	case "identity.password_changed":
		var v eventsv2.PasswordChanged
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID = v.UserID
	case "identity.mfa_changed":
		var v eventsv2.MFAChanged
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID = v.UserID
	case "identity.session_revoked":
		var v eventsv2.SessionRevoked
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID = v.UserID
	case "identity.user_banned":
		var v eventsv2.UserBanned
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID = v.UserID
	case "identity.user_unbanned":
		var v eventsv2.UserUnbanned
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID = v.UserID
	case "billing.payment_accepted":
		var v eventsv2.PaymentAccepted
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID = v.UserID
		p.Vars["order_id"], p.Vars["amount"], p.Vars["currency"] = v.OrderID, v.Amount, v.Currency
	case "billing.order_fulfilled":
		var v eventsv2.OrderFulfilled
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID = v.UserID
		p.Vars["order_id"], p.Vars["amount"], p.Vars["currency"] = v.OrderID, v.Amount, v.Currency
	case "subscription.action_applied":
		var v eventsv2.ActionApplied
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID, p.SubscriptionID, p.Version, p.PeriodEnd = v.UserID, v.SubscriptionID, v.SubscriptionVersion, &v.PeriodEnd
		p.Vars["subscription_id"], p.Vars["period_end"] = v.SubscriptionID, v.PeriodEnd.UTC().Format(time.RFC3339)
	case "subscription.action_rejected":
		var v eventsv2.ActionRejected
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID = v.UserID
		p.Vars["reason"] = v.Reason
	case "subscription.changed":
		var v eventsv2.SubscriptionChanged
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		lifecyclePlan(&p, v.SubscriptionID, v.UserID, v.State, v.Version, v.PeriodEnd)
	case "subscription.expired":
		var v eventsv2.SubscriptionExpired
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		lifecyclePlan(&p, v.SubscriptionID, v.UserID, v.State, v.Version, v.PeriodEnd)
	case "subscription.closed":
		var v eventsv2.SubscriptionClosed
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		lifecyclePlan(&p, v.SubscriptionID, v.UserID, v.State, v.Version, v.PeriodEnd)
	case "subscription.compensated":
		var v eventsv2.SubscriptionCompensated
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		lifecyclePlan(&p, v.SubscriptionID, v.UserID, v.State, v.Version, v.PeriodEnd)
	case "referral.commission_posted":
		var v eventsv2.CommissionPosted
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID = v.UserID
		p.Vars["amount"], p.Vars["currency"] = v.Amount, v.Currency
	case "referral.commission_reversed":
		var v eventsv2.CommissionReversed
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID = v.UserID
		p.Vars["amount"], p.Vars["currency"] = v.Amount, v.Currency
	case "referral.payout_requested":
		var v eventsv2.PayoutRequested
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID = v.UserID
		p.Vars["amount"], p.Vars["currency"], p.Vars["payout_id"], p.Vars["state"] = v.Amount, v.Currency, v.PayoutID, v.State
	case "referral.payout_decided":
		var v eventsv2.PayoutDecided
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID = v.UserID
		p.Vars["amount"], p.Vars["currency"], p.Vars["payout_id"], p.Vars["state"] = v.Amount, v.Currency, v.PayoutID, v.State
	case "referral.payout_cancelled":
		var v eventsv2.PayoutCancelled
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID = v.UserID
		p.Vars["amount"], p.Vars["currency"], p.Vars["payout_id"], p.Vars["state"] = v.Amount, v.Currency, v.PayoutID, v.State
	case "external.readiness_changed":
		var v eventsv2.ExternalReadinessChanged
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID, p.SubscriptionID = v.UserID, v.SubscriptionID
		p.Vars["status"], p.Vars["message"], p.Vars["subscription_id"] = v.Status, v.Message, v.SubscriptionID
	case "external.trading_alert":
		var v eventsv2.ExternalTradingAlert
		if json.Unmarshal(raw, &v) != nil {
			return platform.ErrPoison
		}
		p.UserID, p.SubscriptionID = v.UserID, v.SubscriptionID
		p.Vars["status"], p.Vars["message"], p.Vars["subscription_id"] = v.Status, v.Message, v.SubscriptionID
	default:
		return s.acceptV2(ctx, e, nil, nil)
	}
	intents, err := s.planIntents(ctx, e.EventID, e.EventType, p)
	if err != nil {
		return err
	}
	if e.EventType == "referral.payout_requested" {
		staff, err := s.staffRecipients(ctx, "referrals:payouts:decide")
		if err != nil {
			return err
		}
		for _, user := range staff {
			q := p
			q.UserID = user.UserID
			q.Category = "admin"
			q.RequiredPermission = "referrals:payouts:decide"
			more, err := s.planIntents(ctx, e.EventID, e.EventType, q)
			if err != nil {
				return err
			}
			if len(more) > 0 {
				kept := intents[:0]
				for _, intent := range intents {
					if intent.UserID != user.UserID || intent.Channel != "telegram" {
						kept = append(kept, intent)
					}
				}
				intents = append(kept, more...)
			}
		}
	}
	return s.acceptV2(ctx, e, intents, p.Reminder)
}
func lifecyclePlan(p *deliveryPlan, id, user, state string, version int64, end time.Time) {
	p.UserID, p.SubscriptionID, p.Version, p.PeriodEnd = user, id, version, &end
	p.Vars["subscription_id"], p.Vars["period_end"] = id, end.UTC().Format(time.RFC3339)
	p.Reminder = &reminderPlan{SubscriptionID: id, UserID: user, State: state, Version: version, PeriodEnd: end}
}
func (s *Service) planIntents(ctx context.Context, eventID, eventType string, p deliveryPlan) ([]Intent, error) {
	template, err := s.loadTemplate(ctx, eventType)
	if err != nil {
		return nil, err
	}
	channels := []string{"email"}
	var policy recipientPolicy
	// Reset commands target an existing account, unlike first verification.
	// Resolve only the locale: transactional mail bypasses optional switches.
	if eventType == "notification.password_reset" && p.Locale == "" {
		policy, err = s.recipient(ctx, p.UserID)
		if err != nil {
			return nil, err
		}
		p.Locale = policy.Locale
	}
	if p.Category != "transactional" {
		policy, err = s.recipient(ctx, p.UserID)
		if err != nil {
			return nil, err
		}
		p.Locale = policy.Locale
		channels = []string{"email", "telegram"}
		if p.Category == "trading" || p.Category == "admin" {
			channels = []string{"telegram"}
		}
	}
	subject, text, err := renderTemplate(template, p.Locale, p.Vars)
	if err != nil {
		return nil, platform.ErrPoison
	}
	out := make([]Intent, 0, len(channels))
	for _, channel := range channels {
		target := p.Target
		if p.Category != "transactional" {
			if channel == "email" {
				target = policy.Email
			} else {
				if policy.TelegramChatID == nil {
					continue
				}
				target = *policy.TelegramChatID
			}
		}
		body := Body{To: target, Subject: subject, Text: text, ExpiresAt: p.ExpiresAt, UserID: p.UserID, EventType: eventType, Category: p.Category, BindingVersion: policy.BindingVersion, FlowEventID: eventID, RequiredPermission: p.RequiredPermission, SubscriptionID: p.SubscriptionID, SubscriptionVersion: p.Version, PeriodEnd: p.PeriodEnd}
		id := uuid.NewString()
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		encrypted, err := platform.Encrypt(s.Config.DeliveryKey, id, raw)
		if err != nil {
			return nil, err
		}
		out = append(out, Intent{ID: id, UserID: p.UserID, Channel: channel, EncryptedBody: encrypted, MessageID: "<" + id + "@sso.local>", TemplateVersion: template.Version})
	}
	return out, nil
}
func (s *Service) acceptV2(ctx context.Context, e eventsv2.Envelope, jobs []Intent, reminder *reminderPlan) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	fresh, err := platform.AcceptV2Receipt(ctx, tx, e)
	if err != nil {
		return err
	}
	if !fresh {
		return tx.Commit(ctx)
	}
	tag, err := tx.Exec(ctx, "INSERT INTO notification_v2_receipts(event_id,event_type) VALUES($1,$2) ON CONFLICT DO NOTHING", e.EventID, e.EventType)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	if _, err = tx.Exec(ctx, "INSERT INTO notification_aggregate_versions(producer,aggregate_type,aggregate_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", e.Producer, e.AggregateType, e.AggregateID); err != nil {
		return err
	}
	var version int64
	if err = tx.QueryRow(ctx, "SELECT version FROM notification_aggregate_versions WHERE producer=$1 AND aggregate_type=$2 AND aggregate_id=$3 FOR UPDATE", e.Producer, e.AggregateType, e.AggregateID).Scan(&version); err != nil {
		return err
	}
	if e.AggregateVersion != version+1 {
		return errSequenceGap
	}
	if _, err = tx.Exec(ctx, "UPDATE notification_aggregate_versions SET version=$4 WHERE producer=$1 AND aggregate_type=$2 AND aggregate_id=$3", e.Producer, e.AggregateType, e.AggregateID, e.AggregateVersion); err != nil {
		return err
	}
	legacy, err := tx.Exec(ctx, "INSERT INTO inbox(event_id,event_type) VALUES($1,$2) ON CONFLICT DO NOTHING", e.EventID, e.EventType)
	if err != nil {
		return err
	}
	if legacy.RowsAffected() == 0 {
		var storedType string
		if err = tx.QueryRow(ctx, "SELECT event_type FROM inbox WHERE event_id=$1", e.EventID).Scan(&storedType); err != nil {
			return err
		}
		if storedType != e.EventType {
			return platform.ErrPoison
		}
		return tx.Commit(ctx)
	}
	for _, j := range jobs {
		if _, err = tx.Exec(ctx, "INSERT INTO delivery_jobs(id,event_id,user_id,channel,encrypted_body,message_id,traceparent,template_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(event_id,user_id,channel) DO NOTHING", j.ID, e.EventID, j.UserID, j.Channel, j.EncryptedBody, j.MessageID, e.Traceparent, j.TemplateVersion); err != nil {
			return err
		}
	}
	if err = insertReminderPlans(ctx, tx, reminder); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Service) staffRecipients(ctx context.Context, permission string) ([]internalhttpv2.UserEligibility, error) {
	var out []internalhttpv2.UserEligibility
	cursor := ""
	for {
		query := url.Values{"permission": {permission}, "limit": {"100"}}
		if cursor != "" {
			query.Set("cursor", cursor)
		}
		status, raw, err := s.Client.Call(ctx, "GET", s.Config.IdentityURL+"/internal/v2/notification-recipients?"+query.Encode(), nil, nil)
		var page internalhttpv2.NotificationRecipientsResponse
		if err != nil || status != 200 || json.Unmarshal(raw, &page) != nil {
			return nil, errAuthorityUnavailable
		}
		out = append(out, page.Items...)
		if page.NextCursor == nil {
			return out, nil
		}
		if *page.NextCursor == cursor {
			return nil, errAuthorityUnavailable
		}
		cursor = *page.NextCursor
	}
}
