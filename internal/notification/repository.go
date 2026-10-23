package notification

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
	"user-notification-service/internal/platform"
)

// JobRepository owns the single atomic inbox+jobs boundary and lease fencing.
// Application decisions and provider calls cannot commit this state separately.
type JobRepository interface {
	Accept(context.Context, platform.Envelope, []Intent) error
	Claim(context.Context) (Job, error)
	Finish(context.Context, Job, bool, bool, string) error
}
type Intent struct {
	ID, UserID, Channel, EncryptedBody, MessageID string
	TemplateVersion                               int64
}
type PostgresJobs struct{ DB *pgxpool.Pool }

func (p *PostgresJobs) Accept(ctx context.Context, e platform.Envelope, jobs []Intent) error {
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, "INSERT INTO inbox(event_id,event_type) VALUES($1,$2) ON CONFLICT DO NOTHING", e.EventID, e.EventType)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}
	for _, j := range jobs {
		if j.UserID == "" {
			j.UserID = e.AggregateID
		}
		if _, err = tx.Exec(ctx, "INSERT INTO delivery_jobs(id,event_id,user_id,channel,encrypted_body,message_id,traceparent,template_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(event_id,user_id,channel) DO NOTHING", j.ID, e.EventID, j.UserID, j.Channel, j.EncryptedBody, j.MessageID, e.Traceparent, j.TemplateVersion); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

type DeliveryProvider interface {
	Send(context.Context, Job, Body) (permanent bool, category string, err error)
}
type smtpProvider struct{ Service *Service }

func (p smtpProvider) Send(ctx context.Context, j Job, b Body) (bool, string, error) {
	return p.Service.sendSMTP(ctx, j, b)
}

type telegramProvider struct{ Service *Service }

func (p telegramProvider) Send(ctx context.Context, j Job, b Body) (bool, string, error) {
	return p.Service.sendTelegram(ctx, j, b)
}

type reminderPlan struct {
	SubscriptionID, UserID, State string
	Version                       int64
	PeriodEnd                     time.Time
}

func insertReminderPlans(ctx context.Context, tx pgx.Tx, p *reminderPlan) error {
	if p == nil {
		return nil
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "notification-reminder:"+p.SubscriptionID); err != nil {
		return err
	}
	var newest int64
	if err := tx.QueryRow(ctx, "SELECT coalesce(max(subscription_version),0) FROM notification_reminders WHERE subscription_id=$1", p.SubscriptionID).Scan(&newest); err != nil {
		return err
	}
	if newest > p.Version {
		return nil
	}
	if _, err := tx.Exec(ctx, "UPDATE notification_reminders SET state='cancelled' WHERE subscription_id=$1 AND state='pending' AND (subscription_version<>$2 OR period_end<>$3 OR $4<>'ACTIVE')", p.SubscriptionID, p.Version, p.PeriodEnd, p.State); err != nil {
		return err
	}
	if p.State != "ACTIVE" {
		return nil
	}
	for _, days := range []int{1, 3} {
		due := p.PeriodEnd.Add(-time.Duration(days) * 24 * time.Hour)
		if _, err := tx.Exec(ctx, "INSERT INTO notification_reminders(id,subscription_id,user_id,subscription_version,period_end,days_before,due_at,next_attempt_at) VALUES(gen_random_uuid(),$1,$2,$3,$4,$5,$6,$6) ON CONFLICT(subscription_id,subscription_version,days_before) DO NOTHING", p.SubscriptionID, p.UserID, p.Version, p.PeriodEnd, days, due); err != nil {
			return err
		}
	}
	return nil
}
