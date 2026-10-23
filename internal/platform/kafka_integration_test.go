package platform

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRealKafkaFailedRecordIsReseekedBeforeLaterOffsetCommit(t *testing.T) {
	brokers := os.Getenv("TEST_KAFKA_BROKERS")
	if brokers == "" || os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("disposable TEST_KAFKA_BROKERS and TEST_DATABASE_URL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	group := "regression-" + uuid.NewString()
	// Plaintext is scoped to this test-only disposable broker, never runtime
	// NewConsumer, which requires TLS/SCRAM without a plaintext fallback.
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.ConsumerGroup(group), kgo.ConsumeTopics("identity.events.v1"), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	user, id1, id2 := uuid.NewString(), uuid.NewString(), uuid.NewString()
	payload, _ := json.Marshal(map[string]string{"user_id": user})
	for _, id := range []string{id1, id2} {
		event := Envelope{SchemaVersion: 1, EventID: id, AggregateID: user, EventType: "identity.email_verified", OccurredAt: time.Now(), Payload: payload}
		raw, _ := json.Marshal(event)
		if err = producer.ProduceSync(ctx, &kgo.Record{Topic: "identity.events.v1", Key: []byte(user), Value: raw}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	var attempts atomic.Int32
	processed := make(chan string, 2)
	db, err := pgxpool.New(ctx, os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = Migrate(ctx, db, "notification"); err != nil {
		t.Fatal(err)
	}
	consumer := &Consumer{DB: db, Config: Config{QuarantineKey: make([]byte, 32)}, Kafka: client, Apply: func(ctx context.Context, e Envelope) error {
		if e.EventID == id1 {
			if attempts.Add(1) == 1 {
				return errors.New("transient application failure")
			}
			processed <- id1
		}
		if e.EventID == id2 {
			if attempts.Load() < 2 {
				t.Error("later offset passed failed record")
			}
			processed <- id2
		}
		return nil
	}}
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	for range 2 {
		select {
		case <-processed:
		case <-ctx.Done():
			t.Fatal("failed record did not recover before later record")
		}
	}
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}
