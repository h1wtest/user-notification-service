package platform

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

const commonSQL = `
CREATE TABLE IF NOT EXISTS service_schema(owner text PRIMARY KEY,version integer NOT NULL);
CREATE TABLE IF NOT EXISTS inbox(event_id uuid PRIMARY KEY,event_type text NOT NULL,processed_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS quarantine(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),topic text NOT NULL,partition_id integer NOT NULL,offset_id bigint NOT NULL,encrypted_record text NOT NULL,reason text NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),replayed_at timestamptz,UNIQUE(topic,partition_id,offset_id));
CREATE TABLE IF NOT EXISTS dlq_outbox(id uuid PRIMARY KEY REFERENCES quarantine(id),published_at timestamptz);
CREATE TABLE IF NOT EXISTS quarantine_replays(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),quarantine_id uuid NOT NULL REFERENCES quarantine(id),actor_id uuid NOT NULL,encrypted_event text NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),completed_at timestamptz);
`
const profileSQL = `
CREATE TABLE IF NOT EXISTS profiles(user_id uuid PRIMARY KEY,email text NOT NULL,display_name text,avatar_url text,phone_number text,telegram_chat_id text UNIQUE,updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS notification_settings(user_id uuid PRIMARY KEY REFERENCES profiles(user_id),email_login_alerts boolean NOT NULL DEFAULT true,email_security_alerts boolean NOT NULL DEFAULT true,telegram_login_alerts boolean NOT NULL DEFAULT false,telegram_security_alerts boolean NOT NULL DEFAULT false);
CREATE TABLE IF NOT EXISTS telegram_links(token_hash bytea PRIMARY KEY,user_id uuid NOT NULL REFERENCES profiles(user_id),expires_at timestamptz NOT NULL,consumed_at timestamptz);
CREATE TABLE IF NOT EXISTS telegram_updates(update_id bigint PRIMARY KEY,received_at timestamptz NOT NULL DEFAULT now());
`
const notificationSQL = `
CREATE TABLE IF NOT EXISTS delivery_jobs(id uuid PRIMARY KEY,event_id uuid NOT NULL,user_id uuid NOT NULL,channel text NOT NULL CHECK(channel IN ('email','telegram')),encrypted_body text NOT NULL,message_id text NOT NULL,status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','leased','retry','sent','failed')),attempts integer NOT NULL DEFAULT 0,next_attempt_at timestamptz NOT NULL DEFAULT now(),lease_token uuid,lease_until timestamptz,last_error text,ambiguous boolean NOT NULL DEFAULT false,traceparent text,created_at timestamptz NOT NULL DEFAULT now(),delivered_at timestamptz,UNIQUE(event_id,channel));
CREATE INDEX IF NOT EXISTS delivery_jobs_pending ON delivery_jobs(next_attempt_at) WHERE status IN ('pending','retry','leased');
CREATE TABLE IF NOT EXISTS delivery_attempts(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,job_id uuid NOT NULL REFERENCES delivery_jobs(id),attempt integer NOT NULL,status text NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS replay_audit(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),delivery_id uuid,quarantine_id uuid,actor_id uuid NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
`
const auditSQL = `
CREATE TABLE IF NOT EXISTS audit_events(id uuid PRIMARY KEY,event_type text NOT NULL,actor_id uuid,user_id uuid NOT NULL,created_at timestamptz NOT NULL,traceparent text);
CREATE INDEX IF NOT EXISTS audit_events_time ON audit_events(created_at DESC);
CREATE TABLE IF NOT EXISTS admin_commands(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),actor_id uuid NOT NULL,action text NOT NULL,target_id uuid NOT NULL,status text NOT NULL DEFAULT 'requested',created_at timestamptz NOT NULL DEFAULT now());
`

func Migrate(ctx context.Context, db *pgxpool.Pool, owner string) error {
	sql := map[string]string{"profile": profileSQL, "notification": notificationSQL, "admin": auditSQL}[owner]
	if sql == "" {
		return errors.New("unknown migration owner")
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext($1))", "platform-migration-"+owner); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, commonSQL); err != nil {
		return err
	}
	var count int
	if err = tx.QueryRow(ctx, "SELECT count(*) FROM service_schema WHERE owner<>$1", owner).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return errors.New("database already owned by another service")
	}
	if _, err = tx.Exec(ctx, sql); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO service_schema(owner,version) VALUES($1,1) ON CONFLICT(owner) DO NOTHING", owner); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
