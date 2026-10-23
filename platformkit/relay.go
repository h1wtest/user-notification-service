package platformkit

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type publication struct {
	ID, Token, Topic, Key string
	Raw                   []byte
	Attempts              int
	DLQ                   bool
}

func leaseOutbox(ctx context.Context, db *pgxpool.Pool, owner string) (publication, error) {
	token, err := uuid.NewV7()
	if err != nil {
		return publication{}, err
	}
	p := publication{Token: token.String()}
	err = db.QueryRow(ctx, `WITH candidate AS (
 SELECT o.event_id FROM outbox o
 WHERE o.producer=$1 AND o.published_at IS NULL AND o.next_attempt_at<=now()
 AND (o.lease_until IS NULL OR o.lease_until<=now())
 AND NOT EXISTS(SELECT 1 FROM outbox previous WHERE previous.producer=o.producer AND previous.aggregate_type=o.aggregate_type
 AND previous.aggregate_id=o.aggregate_id AND previous.aggregate_version<o.aggregate_version AND previous.published_at IS NULL)
 ORDER BY o.created_at,o.aggregate_version FOR UPDATE OF o SKIP LOCKED LIMIT 1
) UPDATE outbox o SET lease_token=$2,lease_until=now()+interval '30 seconds',attempts=o.attempts+1
 FROM candidate c WHERE o.event_id=c.event_id RETURNING o.event_id::text,o.topic,o.aggregate_id::text,o.envelope,o.attempts`, owner, p.Token).Scan(&p.ID, &p.Topic, &p.Key, &p.Raw, &p.Attempts)
	return p, err
}

func finishOutbox(ctx context.Context, db *pgxpool.Pool, p publication, publishErr error) error {
	var result int64
	if publishErr == nil {
		tag, err := db.Exec(ctx, "UPDATE outbox SET published_at=now(),lease_token=NULL,lease_until=NULL,last_error=NULL WHERE event_id=$1 AND lease_token=$2 AND lease_until>now() AND published_at IS NULL", p.ID, p.Token)
		if err != nil {
			return err
		}
		result = tag.RowsAffected()
	} else {
		tag, err := db.Exec(ctx, "UPDATE outbox SET lease_token=NULL,lease_until=NULL,next_attempt_at=now()+$3::interval,last_error='broker_unavailable' WHERE event_id=$1 AND lease_token=$2 AND lease_until>now() AND published_at IS NULL", p.ID, p.Token, retryDelay(p.Attempts).String())
		if err != nil {
			return err
		}
		result = tag.RowsAffected()
	}
	if result != 1 {
		return ErrLeaseLost
	}
	return nil
}

func leaseDLQ(ctx context.Context, db *pgxpool.Pool) (publication, error) {
	token, err := uuid.NewV7()
	if err != nil {
		return publication{}, err
	}
	p := publication{Token: token.String(), Topic: "platform.dlq.v2", DLQ: true}
	var topic, reason string
	var partition int32
	var offset int64
	err = db.QueryRow(ctx, `WITH candidate AS (
 SELECT id FROM dlq_outbox WHERE published_at IS NULL AND next_attempt_at<=now() AND (lease_until IS NULL OR lease_until<=now())
 ORDER BY next_attempt_at,id FOR UPDATE SKIP LOCKED LIMIT 1
), leased AS (
 UPDATE dlq_outbox o SET lease_token=$1,lease_until=now()+interval '30 seconds',attempts=o.attempts+1 FROM candidate c WHERE o.id=c.id RETURNING o.id,o.attempts
) SELECT l.id::text,l.attempts,q.topic,q.partition_id,q.offset_id,q.reason FROM leased l JOIN quarantine q ON q.id=l.id`, p.Token).Scan(&p.ID, &p.Attempts, &topic, &partition, &offset, &reason)
	if err != nil {
		return p, err
	}
	p.Key = p.ID
	p.Raw, err = json.Marshal(struct {
		ID        string `json:"id"`
		Topic     string `json:"topic"`
		Partition int32  `json:"partition"`
		Offset    int64  `json:"offset"`
		Reason    string `json:"reason"`
	}{p.ID, topic, partition, offset, reason})
	return p, err
}

func finishDLQ(ctx context.Context, db *pgxpool.Pool, p publication, publishErr error) error {
	var result int64
	if publishErr == nil {
		tag, err := db.Exec(ctx, "UPDATE dlq_outbox SET published_at=now(),lease_token=NULL,lease_until=NULL,last_error=NULL WHERE id=$1 AND lease_token=$2 AND lease_until>now() AND published_at IS NULL", p.ID, p.Token)
		if err != nil {
			return err
		}
		result = tag.RowsAffected()
	} else {
		tag, err := db.Exec(ctx, "UPDATE dlq_outbox SET lease_token=NULL,lease_until=NULL,next_attempt_at=now()+$3::interval,last_error='broker_unavailable' WHERE id=$1 AND lease_token=$2 AND lease_until>now() AND published_at IS NULL", p.ID, p.Token, retryDelay(p.Attempts).String())
		if err != nil {
			return err
		}
		result = tag.RowsAffected()
	}
	if result != 1 {
		return ErrLeaseLost
	}
	return nil
}

func Relay(ctx context.Context, db *pgxpool.Pool, c Config) error {
	if db == nil || c.Name == "" {
		return errors.New("event relay owner/database required")
	}
	opts, err := kafkaOptions(c)
	if err != nil {
		return err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer client.Close()
	for ctx.Err() == nil {
		worked := false
		for _, dlq := range []bool{false, true} {
			var p publication
			if dlq {
				p, err = leaseDLQ(ctx, db)
			} else {
				p, err = leaseOutbox(ctx, db, c.Name)
			}
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			worked = true
			attempt, cancel := context.WithTimeout(ctx, 15*time.Second)
			record, span := publicationRecord(attempt, p)
			publishErr := client.ProduceSync(attempt, record).FirstErr()
			cancel()
			if p.DLQ {
				err = finishDLQ(ctx, db, p, publishErr)
			} else {
				err = finishOutbox(ctx, db, p, publishErr)
			}
			span.finish("kafka.publish", publishErr != nil || err != nil)
			if telemetry := telemetryFrom(ctx); telemetry != nil && (publishErr != nil || err != nil) {
				reason := "broker_unavailable"
				if err != nil {
					reason = "publication_state_unavailable"
				}
				if errors.Is(err, ErrLeaseLost) {
					reason = "lease_lost"
				}
				telemetry.metrics.retry("publish", reason)
			}
			if err != nil && !errors.Is(err, ErrLeaseLost) {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			if publishErr != nil {
				slog.Warn("event publication awaiting broker acknowledgment", "owner", c.Name, "event_id", p.ID, "attempt", p.Attempts)
			}
		}
		if !worked && !pause(ctx, 250*time.Millisecond) {
			return nil
		}
	}
	return nil
}
