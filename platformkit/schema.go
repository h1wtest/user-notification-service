package platformkit

// SchemaSQL is embedded in an owner's versioned migration. Runtime paths never
// execute this SQL. Keep this constant immutable once an owner applies it;
// additions require a new owner migration, not editing historical SQL bytes.
const SchemaSQL = `
CREATE TABLE outbox (
 event_id uuid PRIMARY KEY,event_type text NOT NULL,producer text NOT NULL,
 aggregate_type text NOT NULL,aggregate_id uuid NOT NULL,aggregate_version bigint NOT NULL CHECK(aggregate_version>=1),
 topic text NOT NULL,envelope bytea NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0),next_attempt_at timestamptz NOT NULL DEFAULT now(),
 lease_token uuid,lease_until timestamptz,published_at timestamptz,last_error text,
 UNIQUE(producer,aggregate_type,aggregate_id,aggregate_version)
);
CREATE INDEX outbox_pending ON outbox(next_attempt_at,created_at) WHERE published_at IS NULL;
CREATE TABLE inbox (event_id uuid PRIMARY KEY,event_type text NOT NULL,envelope_hash bytea,processed_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE event_cursors (producer text NOT NULL,aggregate_type text NOT NULL,aggregate_id uuid NOT NULL,version bigint NOT NULL CHECK(version>=0),PRIMARY KEY(producer,aggregate_type,aggregate_id));
CREATE TABLE quarantine (
 id uuid PRIMARY KEY,topic text NOT NULL,partition_id integer NOT NULL,offset_id bigint NOT NULL,
 encrypted_record text NOT NULL,reason text NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),replayed_at timestamptz,
 UNIQUE(topic,partition_id,offset_id)
);
CREATE TABLE dlq_outbox (
 id uuid PRIMARY KEY REFERENCES quarantine(id),attempts integer NOT NULL DEFAULT 0 CHECK(attempts>=0),
 next_attempt_at timestamptz NOT NULL DEFAULT now(),lease_token uuid,lease_until timestamptz,published_at timestamptz,last_error text
);
CREATE INDEX dlq_outbox_pending ON dlq_outbox(next_attempt_at) WHERE published_at IS NULL;
`
