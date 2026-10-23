package platform

func ownerUpgradeSQL(owner string) string {
	switch owner {
	case "profile":
		return ownerOutboxSQL + quarantineUpgradeSQL + profileUpgradeSQL
	case "notification":
		return ownerOutboxSQL + quarantineUpgradeSQL + notificationUpgradeSQL
	case "admin":
		return ownerOutboxSQL + quarantineUpgradeSQL + auditUpgradeSQL
	}
	return ""
}

const quarantineUpgradeSQL = `ALTER TABLE quarantine_replays ADD COLUMN reason text NOT NULL DEFAULT '';`
const profileUpgradeSQL = `
ALTER TABLE profiles ADD COLUMN website_locale text NOT NULL DEFAULT 'en' CHECK(website_locale IN ('en','ru'));
ALTER TABLE profiles ADD COLUMN notification_locale text NOT NULL DEFAULT 'en' CHECK(notification_locale IN ('en','ru'));
ALTER TABLE profiles ADD COLUMN binding_version bigint NOT NULL DEFAULT 0;
ALTER TABLE profiles ADD COLUMN version bigint NOT NULL DEFAULT 1;
ALTER TABLE notification_settings ADD COLUMN email_subscription boolean NOT NULL DEFAULT true;
ALTER TABLE notification_settings ADD COLUMN telegram_subscription boolean NOT NULL DEFAULT false;
ALTER TABLE notification_settings ADD COLUMN telegram_trading boolean NOT NULL DEFAULT false;
ALTER TABLE telegram_links ADD COLUMN session_id text;
ALTER TABLE telegram_links ADD COLUMN candidate_chat_id text;
ALTER TABLE telegram_links ADD COLUMN binding_version bigint NOT NULL DEFAULT 0;
DELETE FROM telegram_links WHERE session_id IS NULL;
ALTER TABLE telegram_links ALTER COLUMN session_id SET NOT NULL;
CREATE UNIQUE INDEX telegram_links_user ON telegram_links(user_id);
CREATE TABLE telegram_unlinks(token_hash bytea PRIMARY KEY,user_id uuid NOT NULL REFERENCES profiles(user_id),chat_id text NOT NULL,binding_version bigint NOT NULL,expires_at timestamptz NOT NULL,consumed_at timestamptz);
CREATE INDEX telegram_unlinks_user ON telegram_unlinks(user_id);
CREATE TABLE profile_telegram_reset_audit(id uuid PRIMARY KEY,actor_id uuid NOT NULL,user_id uuid NOT NULL REFERENCES profiles(user_id),reason text NOT NULL,binding_version bigint NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE external_event_receipts(event_id uuid PRIMARY KEY,integration_id text NOT NULL,body_hash text NOT NULL,accepted_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE external_event_nonces(integration_id text NOT NULL,nonce text NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(integration_id,nonce));
CREATE TABLE profile_aggregate_versions(producer text NOT NULL,aggregate_type text NOT NULL,aggregate_id uuid NOT NULL,version bigint NOT NULL DEFAULT 0,PRIMARY KEY(producer,aggregate_type,aggregate_id));
CREATE TABLE profile_v2_receipts(event_id uuid PRIMARY KEY,event_type text NOT NULL,processed_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX external_event_nonce_expiry ON external_event_nonces(created_at);
`
const notificationUpgradeSQL = `
CREATE TABLE notification_templates(event_type text NOT NULL,version bigint NOT NULL CHECK(version>0),locales jsonb NOT NULL CHECK(jsonb_typeof(locales)='object' AND locales ? 'en' AND locales ? 'ru'),created_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(event_type,version));
ALTER TABLE delivery_jobs ADD COLUMN template_version bigint NOT NULL DEFAULT 0;
ALTER TABLE delivery_jobs ADD COLUMN replay_version bigint NOT NULL DEFAULT 0;
ALTER TABLE replay_audit ADD COLUMN reason text NOT NULL DEFAULT '';
CREATE TABLE notification_aggregate_versions(producer text NOT NULL,aggregate_type text NOT NULL,aggregate_id uuid NOT NULL,version bigint NOT NULL DEFAULT 0,PRIMARY KEY(producer,aggregate_type,aggregate_id));
CREATE TABLE notification_v2_receipts(event_id uuid PRIMARY KEY,event_type text NOT NULL,processed_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE notification_reminders(id uuid PRIMARY KEY,subscription_id uuid NOT NULL,user_id uuid NOT NULL,subscription_version bigint NOT NULL,period_end timestamptz NOT NULL,days_before integer NOT NULL CHECK(days_before IN(1,3)),due_at timestamptz NOT NULL,next_attempt_at timestamptz NOT NULL,state text NOT NULL DEFAULT 'pending' CHECK(state IN('pending','queued','cancelled')),created_at timestamptz NOT NULL DEFAULT now(),UNIQUE(subscription_id,subscription_version,days_before));
CREATE INDEX notification_reminders_due ON notification_reminders(next_attempt_at) WHERE state='pending';
ALTER TABLE delivery_jobs DROP CONSTRAINT delivery_jobs_event_id_channel_key;
ALTER TABLE delivery_jobs ADD CONSTRAINT delivery_jobs_event_recipient_channel_key UNIQUE(event_id,user_id,channel);
`
const auditUpgradeSQL = `
ALTER TABLE admin_commands ADD COLUMN reason text NOT NULL DEFAULT '';
ALTER TABLE admin_commands ADD COLUMN idempotency_key text;
UPDATE admin_commands SET idempotency_key=id::text WHERE idempotency_key IS NULL;
ALTER TABLE admin_commands ALTER COLUMN idempotency_key SET NOT NULL;
ALTER TABLE admin_commands ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE admin_commands ADD COLUMN result jsonb;
CREATE UNIQUE INDEX admin_commands_actor_idempotency ON admin_commands(actor_id,idempotency_key);
ALTER TABLE admin_commands ADD CONSTRAINT admin_commands_status_v2 CHECK(status IN('requested','succeeded','denied','unknown'));
ALTER TABLE admin_commands ADD CONSTRAINT admin_commands_reason_v2 CHECK(octet_length(reason)<=500);
ALTER TABLE admin_commands ADD CONSTRAINT admin_commands_key_v2 CHECK(octet_length(idempotency_key) BETWEEN 1 AND 200);
ALTER TABLE audit_events ALTER COLUMN user_id DROP NOT NULL;
ALTER TABLE audit_events ADD COLUMN aggregate_type text;
ALTER TABLE audit_events ADD COLUMN aggregate_id uuid;
ALTER TABLE audit_events ADD COLUMN aggregate_version bigint;
ALTER TABLE audit_events ADD COLUMN producer text;
CREATE INDEX audit_events_page_v2 ON audit_events(created_at DESC,id DESC);
CREATE INDEX admin_commands_page_v2 ON admin_commands(created_at DESC,id DESC);
CREATE TABLE audit_aggregate_versions(producer text NOT NULL,aggregate_type text NOT NULL,aggregate_id uuid NOT NULL,version bigint NOT NULL DEFAULT 0,PRIMARY KEY(producer,aggregate_type,aggregate_id));
CREATE TABLE audit_v2_receipts(event_id uuid PRIMARY KEY,event_type text NOT NULL,processed_at timestamptz NOT NULL DEFAULT now());
`
