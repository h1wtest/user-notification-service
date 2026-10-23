package platform

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
	"platform-contracts/gen/go/eventsv2"
	"time"
)

const ownerOutboxSQL = `
CREATE TABLE IF NOT EXISTS owner_outbox(event_id uuid PRIMARY KEY,producer text NOT NULL,aggregate_type text NOT NULL,aggregate_id uuid NOT NULL,aggregate_version bigint NOT NULL,topic text NOT NULL,event jsonb NOT NULL,created_at timestamptz NOT NULL DEFAULT now(),published_at timestamptz,UNIQUE(producer,aggregate_type,aggregate_id,aggregate_version));
CREATE TABLE IF NOT EXISTS owner_event_versions(producer text NOT NULL,aggregate_type text NOT NULL,aggregate_id uuid NOT NULL,version bigint NOT NULL,PRIMARY KEY(producer,aggregate_type,aggregate_id));
CREATE INDEX IF NOT EXISTS owner_outbox_pending ON owner_outbox(created_at) WHERE published_at IS NULL;
`

func EnqueueV2(ctx context.Context, tx pgx.Tx, e eventsv2.Envelope) error {
	if e.AggregateVersion < 1 {
		e.AggregateVersion = 1
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", e.EventID); err != nil {
		return err
	}
	var prior bool
	if err := tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM owner_outbox WHERE event_id=$1)", e.EventID).Scan(&prior); err != nil {
		return err
	}
	if prior {
		return nil
	}
	if err := tx.QueryRow(ctx, `INSERT INTO owner_event_versions(producer,aggregate_type,aggregate_id,version) VALUES($1,$2,$3,1) ON CONFLICT(producer,aggregate_type,aggregate_id) DO UPDATE SET version=owner_event_versions.version+1 RETURNING version`, e.Producer, e.AggregateType, e.AggregateID).Scan(&e.AggregateVersion); err != nil {
		return err
	}
	if e.Validate() != nil {
		return ErrPoison
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO owner_outbox(event_id,producer,aggregate_type,aggregate_id,aggregate_version,topic,event) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(event_id) DO NOTHING`, e.EventID, e.Producer, e.AggregateType, e.AggregateID, e.AggregateVersion, e.Topic(), raw)
	return err
}

func (c *Consumer) relayOwnerOutbox(ctx context.Context) {
	for ctx.Err() == nil {
		err := c.publishOwnerEvent(ctx)
		if errors.Is(err, pgx.ErrNoRows) || err != nil {
			if !Pause(ctx, time.Second) {
				return
			}
		}
	}
}
func (c *Consumer) publishOwnerEvent(ctx context.Context) error {
	tx, err := c.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var id, aggregate, topic string
	var raw []byte
	// Only the oldest unpublished aggregate version is eligible; concurrent relays
	// cannot publish a later version while its predecessor waits for broker ack.
	err = tx.QueryRow(ctx, `SELECT o.event_id,o.aggregate_id,o.topic,o.event FROM owner_outbox o WHERE o.published_at IS NULL AND NOT EXISTS(SELECT 1 FROM owner_outbox p WHERE p.producer=o.producer AND p.aggregate_type=o.aggregate_type AND p.aggregate_id=o.aggregate_id AND p.aggregate_version<o.aggregate_version AND p.published_at IS NULL) ORDER BY o.created_at,o.event_id FOR UPDATE OF o SKIP LOCKED LIMIT 1`).Scan(&id, &aggregate, &topic, &raw)
	if err != nil {
		return err
	}
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = c.Kafka.ProduceSync(deadline, &kgo.Record{Topic: topic, Key: []byte(aggregate), Value: raw}).FirstErr(); err != nil {
		return errors.New("owner outbox publish unavailable")
	}
	if _, err = tx.Exec(ctx, "UPDATE owner_outbox SET published_at=now() WHERE event_id=$1", id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
