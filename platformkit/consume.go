package platformkit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"platform-contracts/gen/go/eventsv2"
)

type eventConsumer struct {
	db    *pgxpool.Pool
	key   []byte
	apply func(context.Context, pgx.Tx, eventsv2.Envelope) error
}

// Consume starts at the earliest retained offset. Every valid type in subscribed
// topics reaches apply, including irrelevant types the owner handles as no-op.
// First aggregate version is one: gaps require owner replay or an audited owner
// snapshot migration of event_cursors, never inference from event timestamps.
func Consume(ctx context.Context, db *pgxpool.Pool, c Config, group string, topics []string, apply func(context.Context, pgx.Tx, eventsv2.Envelope) error) error {
	if db == nil || group == "" || len(topics) == 0 || apply == nil {
		return errors.New("consumer database, group, topics and apply required")
	}
	key, err := base64.StdEncoding.DecodeString(os.Getenv("QUARANTINE_ENCRYPTION_KEY"))
	if err != nil || len(key) != 32 {
		return errors.New("QUARANTINE_ENCRYPTION_KEY must be base64 32 bytes")
	}
	opts, err := kafkaOptions(c)
	if err != nil {
		return err
	}
	opts = append(opts, kgo.ConsumerGroup(group), kgo.ConsumeTopics(topics...), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.RebalanceTimeout(30*time.Second))
	if telemetry := telemetryFrom(ctx); telemetry != nil {
		clear := func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			telemetry.metrics.clearLag(partitions)
		}
		opts = append(opts, kgo.OnPartitionsRevoked(clear), kgo.OnPartitionsLost(clear))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	consumer := eventConsumer{db: db, key: key, apply: apply}
	return consumer.run(ctx, client)
}

func (c *eventConsumer) run(ctx context.Context, client *kgo.Client) error {
	defer func() {
		client.AllowRebalance()
		leave, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = client.LeaveGroupContext(leave)
		cancel()
		client.Close()
	}()
	for ctx.Err() == nil {
		fetches := client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return nil
		}
		failed := false
		fetches.EachError(func(topic string, partition int32, _ error) {
			slog.Warn("Kafka partition unavailable", "topic", topic, "partition", partition)
			failed = true
			if telemetry := telemetryFrom(ctx); telemetry != nil {
				telemetry.metrics.retry("consume", "partition_unavailable")
			}
		})
		// Even fetches with partition errors can contain a healthy record whose
		// local fetch position has already advanced; it must not be discarded.
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			if telemetry := telemetryFrom(ctx); telemetry != nil && len(partition.Records) > 0 {
				telemetry.metrics.recordLag(partition.Topic, partition.Partition, partition.HighWatermark-partition.Records[0].Offset)
			}
			for _, record := range partition.Records {
				attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
				attempt, span := consumerSpan(attempt, record)
				err := c.processRecord(attempt, record)
				cancel()
				if err == nil {
					commit, cancel := context.WithTimeout(ctx, 5*time.Second)
					err = client.CommitRecords(commit, record)
					cancel()
				}
				span.finish("kafka.process", err != nil)
				if telemetry := telemetryFrom(ctx); telemetry != nil && err == nil {
					telemetry.metrics.recordLag(partition.Topic, partition.Partition, partition.HighWatermark-record.Offset-1)
				}
				if err != nil {
					client.SetOffsets(map[string]map[int32]kgo.EpochOffset{record.Topic: {record.Partition: {Epoch: record.LeaderEpoch, Offset: record.Offset}}})
					failed = true
					reason := "dependency_retry"
					if errors.Is(err, ErrSequenceGap) {
						reason = "sequence_gap_owner_replay_or_snapshot_required"
					}
					if telemetry := telemetryFrom(ctx); telemetry != nil {
						telemetry.metrics.retry("consume", reason)
					}
					slog.Warn("event offset not acknowledged", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset, "reason", reason)
					break
				}
			}
		})
		// Ownership is held only during bounded DB/commit attempts. Failed local
		// offsets are reset before rebalance; a new owner uses broker commits.
		client.AllowRebalance()
		if failed && !pause(ctx, time.Second) {
			return nil
		}
	}
	return nil
}

func (c *eventConsumer) processRecord(ctx context.Context, record *kgo.Record) error {
	var e eventsv2.Envelope
	if len(record.Value) > bodyLimit || json.Unmarshal(record.Value, &e) != nil || e.Validate() != nil || e.Topic() != record.Topic || !bytes.Equal(record.Key, []byte(e.AggregateID)) {
		return c.quarantine(ctx, record, "invalid_event")
	}
	err := c.applyRecord(ctx, record, e)
	if errors.Is(err, ErrPoison) {
		return c.quarantine(ctx, record, "invalid_application_event")
	}
	return err
}

func (c *eventConsumer) applyRecord(ctx context.Context, record *kgo.Record, e eventsv2.Envelope) error {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// The stream lock also serializes concurrent inbox checks and cursor writes.
	stream := e.Producer + ":" + e.AggregateType + ":" + e.AggregateID
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", stream); err != nil {
		return err
	}
	fresh, err := Inbox(ctx, tx, e.EventID, e.EventType)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(record.Value)
	if !fresh {
		var stored []byte
		if err = tx.QueryRow(ctx, "SELECT envelope_hash FROM inbox WHERE event_id=$1", e.EventID).Scan(&stored); err != nil {
			return err
		}
		if stored != nil && !bytes.Equal(stored, hash[:]) {
			return fmt.Errorf("%w: event ID content collision", ErrPoison)
		}
		return tx.Commit(ctx)
	}
	if _, err = tx.Exec(ctx, "UPDATE inbox SET envelope_hash=$2 WHERE event_id=$1", e.EventID, hash[:]); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO event_cursors(producer,aggregate_type,aggregate_id,version) VALUES($1,$2,$3,0) ON CONFLICT DO NOTHING", e.Producer, e.AggregateType, e.AggregateID); err != nil {
		return err
	}
	var version int64
	if err = tx.QueryRow(ctx, "SELECT version FROM event_cursors WHERE producer=$1 AND aggregate_type=$2 AND aggregate_id=$3 FOR UPDATE", e.Producer, e.AggregateType, e.AggregateID).Scan(&version); err != nil {
		return err
	}
	if version != e.AggregateVersion-1 {
		return fmt.Errorf("%w: producer=%s aggregate_type=%s aggregate_id=%s expected=%d received=%d", ErrSequenceGap, e.Producer, e.AggregateType, e.AggregateID, version+1, e.AggregateVersion)
	}
	if err = c.apply(ctx, tx, e); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "UPDATE event_cursors SET version=$4 WHERE producer=$1 AND aggregate_type=$2 AND aggregate_id=$3", e.Producer, e.AggregateType, e.AggregateID, e.AggregateVersion); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (c *eventConsumer) quarantine(ctx context.Context, record *kgo.Record, reason string) error {
	aad := fmt.Sprintf("%s:%d:%d", record.Topic, record.Partition, record.Offset)
	encrypted, err := encryptRecord(c.key, aad, record.Value)
	if err != nil {
		return err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var quarantineID string
	if err = tx.QueryRow(ctx, `INSERT INTO quarantine(id,topic,partition_id,offset_id,encrypted_record,reason) VALUES($1,$2,$3,$4,$5,$6)
ON CONFLICT(topic,partition_id,offset_id) DO UPDATE SET reason=quarantine.reason RETURNING id::text`, id.String(), record.Topic, record.Partition, record.Offset, encrypted, reason).Scan(&quarantineID); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO dlq_outbox(id) VALUES($1) ON CONFLICT DO NOTHING", quarantineID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
