// SPDX-License-Identifier: MIT
package audit

import (
	"context"
	"encoding/json"

	"platform-contracts/gen/go/eventsv2"
	"user-notification-service/internal/platform"
)

// ApplyV2 persists metadata only. Payloads remain owner-local: even public
// domain events can contain IPs, financial evidence or incident narratives.
func (s *Service) ApplyV2(ctx context.Context, e eventsv2.Envelope) error {
	if e.Validate() != nil || e.EncryptedPayload != "" {
		return platform.ErrPoison
	}
	row, err := projectionMetadata(e)
	if err != nil {
		return platform.ErrPoison
	}
	return s.Audit.ProjectMetadata(ctx, row, e.Traceparent)
}
func projectionMetadata(e eventsv2.Envelope) (AuditEvent, error) {
	row := AuditEvent{ID: e.EventID, EventType: e.EventType, AggregateType: &e.AggregateType, AggregateID: &e.AggregateID, AggregateVersion: &e.AggregateVersion, Producer: &e.Producer, CreatedAt: e.OccurredAt, Envelope: &e}
	var user, actor string
	var err error
	switch e.EventType {
	case "billing.payment_reviewed":
		var p eventsv2.PaymentReviewed
		err = json.Unmarshal(e.Payload, &p)
		if err == nil && p.DecisionID != e.AggregateID {
			return row, platform.ErrPoison
		}
		actor = p.ActorUserID
		user = p.UserID
		row.Operation = &p.Operation
		row.Reason = &p.Reason
	case "identity.admin_action_recorded":
		var p eventsv2.AdminActionRecorded
		err = json.Unmarshal(e.Payload, &p)
		if err == nil && (p.AuditID != e.EventID || p.AuditID != e.AggregateID) {
			return row, platform.ErrPoison
		}
		actor = p.ActorUserID
		row.Operation = &p.Operation
		row.TargetType = &p.TargetType
		row.TargetID = &p.TargetID
		row.Reason = &p.Reason
	case "identity.user_registered":
		var p eventsv2.UserRegistered
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "identity.user_logged_in":
		var p eventsv2.UserLoggedIn
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "identity.email_verified":
		var p eventsv2.EmailVerified
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "identity.password_changed":
		var p eventsv2.PasswordChanged
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "identity.mfa_changed":
		var p eventsv2.MFAChanged
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "identity.session_revoked":
		var p eventsv2.SessionRevoked
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "identity.user_banned":
		var p eventsv2.UserBanned
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "identity.user_unbanned":
		var p eventsv2.UserUnbanned
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "catalog.revision_published":
		var p eventsv2.RevisionPublished
		err = json.Unmarshal(e.Payload, &p)
		actor = p.ActorUserID
	case "catalog.revision_archived":
		var p eventsv2.RevisionArchived
		err = json.Unmarshal(e.Payload, &p)
		actor = p.ActorUserID
	case "billing.payment_accepted":
		var p eventsv2.PaymentAccepted
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "billing.order_fulfilled":
		var p eventsv2.OrderFulfilled
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "subscription.action_applied":
		var p eventsv2.ActionApplied
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "subscription.action_rejected":
		var p eventsv2.ActionRejected
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "subscription.changed":
		var p eventsv2.SubscriptionChanged
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "subscription.expired":
		var p eventsv2.SubscriptionExpired
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "subscription.closed":
		var p eventsv2.SubscriptionClosed
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "subscription.compensated":
		var p eventsv2.SubscriptionCompensated
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "referral.account_created":
		var p eventsv2.ReferralAccountCreated
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "referral.claim_prepared":
		var p eventsv2.ClaimPrepared
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "referral.quote_created":
		var p eventsv2.QuoteCreated
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "referral.attribution_finalized":
		var p eventsv2.AttributionFinalized
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "referral.rate_changed":
		var p eventsv2.RateChanged
		err = json.Unmarshal(e.Payload, &p)
		if p.UserID != nil {
			user = *p.UserID
		}
	case "referral.commission_posted":
		var p eventsv2.CommissionPosted
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "referral.commission_reversed":
		var p eventsv2.CommissionReversed
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "referral.payout_requested":
		var p eventsv2.PayoutRequested
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "referral.payout_decided":
		var p eventsv2.PayoutDecided
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "referral.payout_cancelled":
		var p eventsv2.PayoutCancelled
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "profile.updated":
		var p eventsv2.ProfileUpdated
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "profile.telegram_binding_changed":
		var p eventsv2.TelegramBindingChanged
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "external.readiness_changed":
		var p eventsv2.ExternalReadinessChanged
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "external.trading_alert":
		var p eventsv2.ExternalTradingAlert
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
	case "notification.delivery_replayed":
		var p eventsv2.DeliveryReplayed
		err = json.Unmarshal(e.Payload, &p)
		user = p.UserID
		actor = p.ActorID
	case "notification.template_published":
		var p eventsv2.TemplatePublished
		err = json.Unmarshal(e.Payload, &p)
		actor = p.ActorID
	}
	// A newly introduced, valid irrelevant event still receives an inbox and
	// metadata row. Never block a topic's contiguous stream with a closed
	// business handler list; generated envelope validation remains authority.
	if user != "" {
		row.UserID = &user
	}
	if actor != "" {
		row.ActorID = &actor
	}
	return row, err
}
