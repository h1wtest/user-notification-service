package platform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"log/slog"
	"platform-contracts/gen/go/events"
	"time"
)

var ErrPoison = errors.New("invalid_event")

type Consumer struct {
	DB     *pgxpool.Pool
	Config Config
	Kafka  *kgo.Client
	Apply  func(context.Context, Envelope) error
}

func NewConsumer(db *pgxpool.Pool, c Config, group string, topics []string, apply func(context.Context, Envelope) error) (*Consumer, error) {
	security, err := KafkaSecurity(c)
	if err != nil {
		return nil, err
	}
	opts := []kgo.Opt{kgo.SeedBrokers(c.Brokers...), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topics...), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, p map[string][]int32) { ClearLag(p) }), kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, p map[string][]int32) { ClearLag(p) })}
	opts = append(opts, security...)
	k, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Consumer{db, c, k, apply}, nil
}
func Quarantine(ctx context.Context, db *pgxpool.Pool, c Config, topic string, partition int32, offset int64, raw []byte, reason string) error {
	aad := fmt.Sprintf("%s:%d:%d", topic, partition, offset)
	encrypted, err := Encrypt(c.QuarantineKey, aad, raw)
	if err != nil {
		return err
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var id string
	err = tx.QueryRow(ctx, "INSERT INTO quarantine(topic,partition_id,offset_id,encrypted_record,reason) VALUES($1,$2,$3,$4,$5) ON CONFLICT(topic,partition_id,offset_id) DO UPDATE SET reason=EXCLUDED.reason RETURNING id", topic, partition, offset, encrypted, reason).Scan(&id)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO dlq_outbox(id) VALUES($1) ON CONFLICT DO NOTHING", id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (c *Consumer) Run(ctx context.Context) error {
	defer func() { c.Kafka.AllowRebalance(); c.Kafka.Close() }()
	relayDone := make(chan struct{})
	go func() { defer close(relayDone); c.relayDLQ(ctx) }()
	defer func() { <-relayDone }()
	for ctx.Err() == nil {
		fetches := c.Kafka.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return nil
		}
		// Fetch errors are per partition. Healthy records in this same fetch
		// must still be processed: Poll already advances their local positions.
		fetches.EachError(func(topic string, partition int32, err error) {
			slog.Warn("Kafka partition unavailable", "topic", topic, "partition", partition)
		})
		failed := false
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			RecordLag(p.Topic, p.Partition, p.HighWatermark-p.Records[0].Offset)
			for _, record := range p.Records {
				attemptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				err := c.processRecord(attemptCtx, record)
				cancel()
				if err == nil {
					commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					err = c.Kafka.CommitRecords(commitCtx, record)
					cancel()
				}
				if err == nil {
					RecordLag(p.Topic, p.Partition, p.HighWatermark-record.Offset-1)
				}
				if err != nil {
					// Rebalances are blocked only for this bounded attempt. Reset
					// before releasing ownership; if revoked, the next assignment
					// starts at the last committed broker offset instead. Never
					// commit a later record of this partition past this failure.
					c.Kafka.SetOffsets(map[string]map[int32]kgo.EpochOffset{
						record.Topic: {record.Partition: {Epoch: record.LeaderEpoch, Offset: record.Offset}},
					})
					failed = true
					slog.Warn("event processing retry", "topic", record.Topic, "partition", record.Partition, "offset", record.Offset)
					break
				}
			}
		})
		c.Kafka.AllowRebalance()
		if failed || len(fetches.Errors()) > 0 {
			if !Pause(ctx, time.Second) {
				return nil
			}
		}
	}
	return nil
}

func (c *Consumer) processRecord(ctx context.Context, record *kgo.Record) error {
	var event Envelope
	if err := json.Unmarshal(record.Value, &event); err != nil {
		return Quarantine(ctx, c.DB, c.Config, record.Topic, record.Partition, record.Offset, record.Value, "invalid_envelope")
	}
	expected, err := events.Topic(event.EventType)
	if err != nil || expected != record.Topic {
		return Quarantine(ctx, c.DB, c.Config, record.Topic, record.Partition, record.Offset, record.Value, "invalid_event")
	}
	spanCtx, _, finish := StartSpan(ctx, event.Traceparent, "kafka.process "+event.EventType)
	err = c.Apply(spanCtx, event)
	finish(err != nil)
	if errors.Is(err, ErrPoison) {
		return Quarantine(ctx, c.DB, c.Config, record.Topic, record.Partition, record.Offset, record.Value, "invalid_event")
	}
	return err
}
func (c *Consumer) relayDLQ(ctx context.Context) {
	for ctx.Err() == nil {
		tx, err := c.DB.Begin(ctx)
		if err != nil {
			if !Pause(ctx, time.Second) {
				return
			}
			continue
		}
		var id, topic, reason string
		var partition int32
		var offset int64
		err = tx.QueryRow(ctx, "SELECT q.id,q.topic,q.partition_id,q.offset_id,q.reason FROM dlq_outbox o JOIN quarantine q ON q.id=o.id WHERE o.published_at IS NULL ORDER BY q.created_at FOR UPDATE OF o SKIP LOCKED LIMIT 1").Scan(&id, &topic, &partition, &offset, &reason)
		if err != nil {
			_ = tx.Rollback(ctx)
			if !Pause(ctx, time.Second) {
				return
			}
			continue
		}
		// Original bytes remain encrypted in owner DB; broker DLQ has only metadata,
		// so sensitive command contents never appear in shared diagnostics.
		b, _ := json.Marshal(map[string]any{"id": id, "topic": topic, "partition": partition, "offset": offset, "reason": reason})
		timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = c.Kafka.ProduceSync(timeout, &kgo.Record{Topic: "platform.dlq.v1", Key: []byte(id), Value: b}).FirstErr()
		cancel()
		if err == nil {
			_, err = tx.Exec(ctx, "UPDATE dlq_outbox SET published_at=now() WHERE id=$1", id)
		}
		if err == nil {
			err = tx.Commit(ctx)
		} else {
			_ = tx.Rollback(ctx)
		}
		if err != nil {
			if !Pause(ctx, time.Second) {
				return
			}
		}
	}
}
func Pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func Validate(e Envelope) error {
	if e.Validate() != nil {
		return ErrPoison
	}
	if len(e.Payload) > 0 {
		var p struct {
			UserID string `json:"user_id"`
		}
		if json.Unmarshal(e.Payload, &p) != nil || p.UserID != e.AggregateID {
			return ErrPoison
		}
	}
	return nil
}
