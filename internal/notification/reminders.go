package notification

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"net/url"
	"platform-contracts/gen/go/internalhttpv2"
	"strconv"
	"time"
)

type subscriptionState struct {
	ID, UserID, State string
	Version           int64
	PeriodEnd         time.Time
	Allowed           bool
}

func (s *Service) subscription(ctx context.Context, id string) (subscriptionState, error) {
	status, raw, err := s.Client.Call(ctx, "GET", s.Config.SubscriptionURL+"/internal/v2/subscriptions/"+url.PathEscape(id), nil, nil)
	var current internalhttpv2.SubscriptionEligibility
	if err != nil || status != 200 || json.Unmarshal(raw, &current) != nil || current.ID != id {
		return subscriptionState{}, errAuthorityUnavailable
	}
	return subscriptionState{ID: current.ID, UserID: current.UserID, State: current.State, Version: current.Version, PeriodEnd: current.CurrentPeriodEnd, Allowed: current.Allowed}, nil
}

type scheduledReminder struct {
	ID, SubscriptionID, UserID string
	Version                    int64
	PeriodEnd                  time.Time
	Days                       int
}

func reminderCurrent(r scheduledReminder, current subscriptionState, now time.Time) bool {
	return current.ID == r.SubscriptionID && current.UserID == r.UserID && current.Version == r.Version && current.State == "ACTIVE" && current.PeriodEnd.Equal(r.PeriodEnd) && now.Before(r.PeriodEnd)
}
func (s *Service) pumpReminders(ctx context.Context) error {
	if s.DB == nil {
		return nil
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var r scheduledReminder
	err = tx.QueryRow(ctx, "SELECT id,subscription_id,user_id,subscription_version,period_end,days_before FROM notification_reminders WHERE state='pending' AND next_attempt_at<=now() ORDER BY next_attempt_at FOR UPDATE SKIP LOCKED LIMIT 1").Scan(&r.ID, &r.SubscriptionID, &r.UserID, &r.Version, &r.PeriodEnd, &r.Days)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	deadline, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	current, err := s.subscription(deadline, r.SubscriptionID)
	if err != nil {
		if _, updateErr := tx.Exec(ctx, "UPDATE notification_reminders SET next_attempt_at=now()+interval '1 minute' WHERE id=$1", r.ID); updateErr != nil {
			return updateErr
		}
		return tx.Commit(ctx)
	}
	if !reminderCurrent(r, current, time.Now()) {
		if _, err = tx.Exec(ctx, "UPDATE notification_reminders SET state='cancelled' WHERE id=$1", r.ID); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	plan := deliveryPlan{UserID: r.UserID, Category: "subscription", SubscriptionID: r.SubscriptionID, Version: r.Version, PeriodEnd: &r.PeriodEnd, ExpiresAt: &r.PeriodEnd, Vars: map[string]string{"subscription_id": r.SubscriptionID, "period_end": r.PeriodEnd.UTC().Format(time.RFC3339), "days": strconv.Itoa(r.Days)}}
	jobs, err := s.planIntents(deadline, r.ID, "subscription.expiry_reminder", plan)
	if err != nil {
		if _, updateErr := tx.Exec(ctx, "UPDATE notification_reminders SET next_attempt_at=now()+interval '1 minute' WHERE id=$1", r.ID); updateErr != nil {
			return updateErr
		}
		return tx.Commit(ctx)
	}
	if _, err = tx.Exec(ctx, "INSERT INTO inbox(event_id,event_type) VALUES($1,'subscription.expiry_reminder') ON CONFLICT DO NOTHING", r.ID); err != nil {
		return err
	}
	for _, j := range jobs {
		if _, err = tx.Exec(ctx, "INSERT INTO delivery_jobs(id,event_id,user_id,channel,encrypted_body,message_id,template_version) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(event_id,user_id,channel) DO NOTHING", j.ID, r.ID, j.UserID, j.Channel, j.EncryptedBody, j.MessageID, j.TemplateVersion); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, "UPDATE notification_reminders SET state='queued' WHERE id=$1", r.ID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
