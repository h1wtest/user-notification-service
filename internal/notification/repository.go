package notification

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"user-notification-service/internal/platform"
)

// JobRepository owns the single atomic inbox+jobs boundary and lease fencing.
// Application decisions and provider calls cannot commit this state separately.
type JobRepository interface {
	Accept(context.Context, platform.Envelope, []Intent) error
	Claim(context.Context) (Job, error)
	Finish(context.Context, Job, bool, bool, string) error
}
type Intent struct{ ID, Channel, EncryptedBody, MessageID string }
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
		if _, err = tx.Exec(ctx, "INSERT INTO delivery_jobs(id,event_id,user_id,channel,encrypted_body,message_id,traceparent) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(event_id,channel) DO NOTHING", j.ID, e.EventID, e.AggregateID, j.Channel, j.EncryptedBody, j.MessageID, e.Traceparent); err != nil {
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
