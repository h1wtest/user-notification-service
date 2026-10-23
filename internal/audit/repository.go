package audit

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"platform-contracts/gen/go/eventsv2"
	"user-notification-service/internal/platform"
)

var ErrIdempotencyConflict = errors.New("idempotency_conflict")
var ErrCommandNotFound = errors.New("command_not_found")

var ErrAggregateGap = errors.New("aggregate_version_gap")

type Command struct {
	ID             string          `json:"command_id"`
	ActorID        string          `json:"actor_id"`
	Action         string          `json:"action"`
	TargetID       string          `json:"target_id"`
	Reason         string          `json:"reason"`
	IdempotencyKey string          `json:"-"`
	Status         string          `json:"status"`
	Result         json.RawMessage `json:"result,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type AuditEvent struct {
	ID               string             `json:"id"`
	EventType        string             `json:"event_type"`
	ActorID          *string            `json:"actor_id"`
	UserID           *string            `json:"user_id"`
	AggregateType    *string            `json:"aggregate_type,omitempty"`
	AggregateID      *string            `json:"aggregate_id,omitempty"`
	AggregateVersion *int64             `json:"aggregate_version,omitempty"`
	Producer         *string            `json:"producer,omitempty"`
	CreatedAt        time.Time          `json:"created_at"`
	Reason           *string            `json:"reason,omitempty"`
	Operation        *string            `json:"operation,omitempty"`
	TargetType       *string            `json:"target_type,omitempty"`
	TargetID         *string            `json:"target_id,omitempty"`
	Envelope         *eventsv2.Envelope `json:"-"`
}

type Repository interface {
	Project(context.Context, platform.Envelope) error
	ProjectMetadata(context.Context, AuditEvent, string) error
	Admit(context.Context, string, string, string, string, string) (Command, bool, error)
	Lookup(context.Context, string) (Command, error)
	Finish(context.Context, string, string, json.RawMessage) error
	List(context.Context, Page) ([]AuditEvent, error)
}
type PostgresAudit struct{ DB *pgxpool.Pool }

func (p *PostgresAudit) Project(ctx context.Context, e platform.Envelope) error {
	user := e.AggregateID
	return p.ProjectMetadata(ctx, AuditEvent{ID: e.EventID, EventType: e.EventType, UserID: &user, CreatedAt: e.OccurredAt}, e.Traceparent)
}
func (p *PostgresAudit) ProjectMetadata(ctx context.Context, e AuditEvent, trace string) error {
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if e.Envelope != nil {
		fresh, err := platform.AcceptV2Receipt(ctx, tx, *e.Envelope)
		if err != nil {
			return err
		}
		if !fresh {
			return tx.Commit(ctx)
		}
	}
	receiptSQL := "INSERT INTO inbox(event_id,event_type) VALUES($1,$2) ON CONFLICT DO NOTHING"
	if e.AggregateVersion != nil {
		receiptSQL = "INSERT INTO audit_v2_receipts(event_id,event_type) VALUES($1,$2) ON CONFLICT DO NOTHING"
	}
	tag, err := tx.Exec(ctx, receiptSQL, e.ID, e.EventType)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		var recordedType string
		table := "inbox"
		if e.AggregateVersion != nil {
			table = "audit_v2_receipts"
		}
		if err := tx.QueryRow(ctx, "SELECT event_type FROM "+table+" WHERE event_id=$1", e.ID).Scan(&recordedType); err != nil {
			return err
		}
		if recordedType != e.EventType {
			return platform.ErrPoison
		}
		return tx.Commit(ctx)
	}
	if e.AggregateVersion != nil {
		if e.Producer == nil || e.AggregateType == nil || e.AggregateID == nil {
			return errors.New("incomplete_aggregate_metadata")
		}
		_, err = tx.Exec(ctx, `INSERT INTO audit_aggregate_versions(producer,aggregate_type,aggregate_id,version) VALUES($1,$2,$3,0) ON CONFLICT DO NOTHING`, e.Producer, e.AggregateType, e.AggregateID)
		if err != nil {
			return err
		}
		var version int64
		err = tx.QueryRow(ctx, `SELECT version FROM audit_aggregate_versions WHERE producer=$1 AND aggregate_type=$2 AND aggregate_id=$3 FOR UPDATE`, e.Producer, e.AggregateType, e.AggregateID).Scan(&version)
		if err != nil {
			return err
		}
		if *e.AggregateVersion != version+1 {
			return ErrAggregateGap
		}
		_, err = tx.Exec(ctx, `UPDATE audit_aggregate_versions SET version=$4 WHERE producer=$1 AND aggregate_type=$2 AND aggregate_id=$3`, e.Producer, e.AggregateType, e.AggregateID, *e.AggregateVersion)
		if err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events(id,event_type,actor_id,user_id,created_at,traceparent,aggregate_type,aggregate_id,aggregate_version,producer,operation,target_type,target_id,reason) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT DO NOTHING`, e.ID, e.EventType, e.ActorID, e.UserID, e.CreatedAt, trace, e.AggregateType, e.AggregateID, e.AggregateVersion, e.Producer, e.Operation, e.TargetType, e.TargetID, e.Reason)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

const commandColumns = "id::text,actor_id::text,action,target_id::text,reason,idempotency_key,status,result,created_at,updated_at"

func scanCommand(row pgx.Row) (Command, error) {
	var c Command
	err := row.Scan(&c.ID, &c.ActorID, &c.Action, &c.TargetID, &c.Reason, &c.IdempotencyKey, &c.Status, &c.Result, &c.CreatedAt, &c.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, ErrCommandNotFound
	}
	c.CreatedAt = c.CreatedAt.UTC()
	c.UpdatedAt = c.UpdatedAt.UTC()
	return c, err
}
func (p *PostgresAudit) Admit(ctx context.Context, actor, action, target, reason, key string) (Command, bool, error) {
	if strings.TrimSpace(reason) == "" || len(reason) > 500 || strings.TrimSpace(key) == "" || len(key) > 200 {
		return Command{}, false, errors.New("invalid_command")
	}
	id := uuid.NewString()
	c, err := scanCommand(p.DB.QueryRow(ctx, `INSERT INTO admin_commands(id,actor_id,action,target_id,reason,idempotency_key) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(actor_id,idempotency_key) DO NOTHING RETURNING `+commandColumns, id, actor, action, target, reason, key))
	if err == nil {
		return c, true, nil
	}
	if !errors.Is(err, ErrCommandNotFound) {
		return Command{}, false, err
	}
	c, err = scanCommand(p.DB.QueryRow(ctx, "SELECT "+commandColumns+" FROM admin_commands WHERE actor_id=$1 AND idempotency_key=$2", actor, key))
	if err != nil {
		return Command{}, false, err
	}
	if c.Action != action || c.TargetID != target || c.Reason != reason {
		return Command{}, false, ErrIdempotencyConflict
	}
	return c, false, nil
}
func (p *PostgresAudit) Lookup(ctx context.Context, id string) (Command, error) {
	return scanCommand(p.DB.QueryRow(ctx, "SELECT "+commandColumns+" FROM admin_commands WHERE id=$1", id))
}
func (p *PostgresAudit) Finish(ctx context.Context, id, status string, result json.RawMessage) error {
	if status != "succeeded" && status != "denied" && status != "unknown" {
		return errors.New("invalid_command_status")
	}
	_, err := p.DB.Exec(ctx, `UPDATE admin_commands SET status=$2,result=$3,updated_at=now() WHERE id=$1 AND status IN ('requested','unknown')`, id, status, result)
	return err
}
func (p *PostgresAudit) List(ctx context.Context, page Page) ([]AuditEvent, error) {
	var before any
	if !page.Before.IsZero() {
		before = page.Before
	}
	rows, err := p.DB.Query(ctx, `SELECT id::text,event_type,actor_id::text,user_id::text,aggregate_type,aggregate_id::text,aggregate_version,producer,created_at,reason,operation,target_type,target_id FROM (
        SELECT id,event_type,actor_id,user_id,aggregate_type,aggregate_id,aggregate_version,producer,created_at,reason,operation,target_type,target_id FROM audit_events
        UNION ALL
        SELECT id,'admin.'||action||'.'||status,actor_id,target_id,'admin_command',id,NULL::bigint,'admin',created_at,reason,action,'user',target_id::text FROM admin_commands
    ) events WHERE ($1::timestamptz IS NULL OR (created_at,id)<($1::timestamptz,$2::uuid)) ORDER BY created_at DESC,id DESC LIMIT $3`, before, nullableID(page.BeforeID), page.Limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AuditEvent{}
	for rows.Next() {
		var e AuditEvent
		if err = rows.Scan(&e.ID, &e.EventType, &e.ActorID, &e.UserID, &e.AggregateType, &e.AggregateID, &e.AggregateVersion, &e.Producer, &e.CreatedAt, &e.Reason, &e.Operation, &e.TargetType, &e.TargetID); err != nil {
			return nil, err
		}
		e.CreatedAt = e.CreatedAt.UTC()
		out = append(out, e)
	}
	return out, rows.Err()
}
func nullableID(id string) any {
	if id == "" {
		return nil
	}
	return id
}
