package audit

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"user-notification-service/internal/platform"
)

type Repository interface {
	Project(context.Context, platform.Envelope) error
	Request(context.Context, string, string, string) (string, error)
	Complete(context.Context, string, string) error
}
type PostgresAudit struct{ DB *pgxpool.Pool }

func (p *PostgresAudit) Project(ctx context.Context, e platform.Envelope) error {
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
	if _, err = tx.Exec(ctx, "INSERT INTO audit_events(id,event_type,user_id,created_at,traceparent) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING", e.EventID, e.EventType, e.AggregateID, e.OccurredAt, e.Traceparent); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (p *PostgresAudit) Request(ctx context.Context, actor, action, target string) (string, error) {
	var id string
	err := p.DB.QueryRow(ctx, "INSERT INTO admin_commands(actor_id,action,target_id) VALUES($1,$2,$3) RETURNING id", actor, action, target).Scan(&id)
	return id, err
}
func (p *PostgresAudit) Complete(ctx context.Context, id, status string) error {
	_, err := p.DB.Exec(ctx, "UPDATE admin_commands SET status=$2 WHERE id=$1", id, status)
	return err
}
