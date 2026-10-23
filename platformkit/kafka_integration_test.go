package platformkit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"platform-contracts/gen/go/eventsv2"
)

func kafkaFixture(t *testing.T) Config {
	t.Helper()
	if os.Getenv("TEST_KAFKA_BROKERS") == "" {
		t.Skip("disposable TLS/SCRAM TEST_KAFKA_BROKERS required")
	}
	return Config{Name: "catalog", Brokers: strings.Split(os.Getenv("TEST_KAFKA_BROKERS"), ","), KafkaCA: os.Getenv("TEST_KAFKA_CA"), KafkaUsername: os.Getenv("TEST_KAFKA_USERNAME"), KafkaPassword: os.Getenv("TEST_KAFKA_PASSWORD")}
}

func brokerCommittedOffset(ctx context.Context, client *kgo.Client, group, topic string, partition int32) (int64, error) {
	request := kmsg.NewPtrOffsetFetchRequest()
	request.Group = group
	request.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: []int32{partition}}}
	groupRequest := kmsg.NewOffsetFetchRequestGroup()
	groupRequest.Group = group
	groupRequest.Topics = []kmsg.OffsetFetchRequestGroupTopic{{Topic: topic, Partitions: []int32{partition}}}
	request.Groups = []kmsg.OffsetFetchRequestGroup{groupRequest}
	response, err := request.RequestWith(ctx, client)
	if err != nil {
		return 0, err
	}
	if response.ErrorCode != 0 {
		return 0, fmt.Errorf("offset fetch group error %d", response.ErrorCode)
	}
	for _, g := range response.Groups {
		if g.ErrorCode != 0 {
			return 0, fmt.Errorf("offset fetch group error %d", g.ErrorCode)
		}
		for _, t := range g.Topics {
			for _, p := range t.Partitions {
				if p.Partition == partition {
					if p.ErrorCode != 0 {
						return 0, fmt.Errorf("offset fetch partition error %d", p.ErrorCode)
					}
					return p.Offset, nil
				}
			}
		}
	}
	for _, t := range response.Topics {
		for _, p := range t.Partitions {
			if p.Partition == partition {
				if p.ErrorCode != 0 {
					return 0, fmt.Errorf("offset fetch partition error %d", p.ErrorCode)
				}
				return p.Offset, nil
			}
		}
	}
	return 0, errors.New("broker offset response omitted partition")
}

func TestKafkaCancellationHandsOffFailedPartitionWithoutCommitPastTransaction(t *testing.T) {
	config := kafkaFixture(t)
	db := migratedDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if _, err := db.Exec(ctx, "CREATE TABLE kafka_effects(event_id uuid PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	opts, err := kafkaOptions(config)
	if err != nil {
		t.Fatal(err)
	}
	// OffsetFetch v10 switches names to topic UUIDs; use explicitly named v7
	// for this independent broker commit observation, with other limits stable.
	versions := kversion.Stable()
	versions.SetMaxKeyVersion(9, 7)
	producerOpts := append(append([]kgo.Opt{}, opts...), kgo.MaxVersions(versions))
	producer, err := kgo.NewClient(producerOpts...)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	aggregate := uuid.NewString()
	first := fixtureEvent(t, aggregate, 1)
	second := fixtureEvent(t, aggregate, 2)
	firstRecord, secondRecord := fixtureRecord(t, first, 0), fixtureRecord(t, second, 0)
	if err = producer.ProduceSync(ctx, firstRecord, secondRecord).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if firstRecord.Partition != secondRecord.Partition || secondRecord.Offset != firstRecord.Offset+1 {
		t.Fatal("target records are not adjacent in one partition")
	}
	group := "kit-boundary-" + uuid.NewString()
	consumerOpts := append(append([]kgo.Opt{}, opts...), kgo.ConsumerGroup(group), kgo.ConsumeTopics("identity.events.v2"), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.RebalanceTimeout(30*time.Second))
	firstClient, err := kgo.NewClient(consumerOpts...)
	if err != nil {
		t.Fatal(err)
	}
	firstCtx, stopFirst := context.WithCancel(ctx)
	defer stopFirst()
	blocked := make(chan struct{})
	firstConsumer := eventConsumer{db: db, key: make([]byte, 32), apply: func(ctx context.Context, tx pgx.Tx, e eventsv2.Envelope) error {
		if e.AggregateID != aggregate {
			return nil
		}
		if e.EventID != first.EventID {
			return errors.New("later event passed uncommitted predecessor")
		}
		if _, err := tx.Exec(ctx, "INSERT INTO kafka_effects VALUES($1)", e.EventID); err != nil {
			return err
		}
		close(blocked)
		<-ctx.Done()
		return ctx.Err()
	}}
	firstDone := make(chan error, 1)
	go func() { firstDone <- firstConsumer.run(firstCtx, firstClient) }()
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("first owner did not reach uncommitted effect")
	}
	assertNotCommitted := func() {
		t.Helper()
		offset, err := brokerCommittedOffset(ctx, producer, group, "identity.events.v2", firstRecord.Partition)
		if err != nil {
			t.Fatal(err)
		}
		if offset > firstRecord.Offset {
			t.Fatalf("broker acknowledged offset %d while effect uncommitted at %d", offset, firstRecord.Offset)
		}
		var count int
		if err = db.QueryRow(ctx, "SELECT count(*) FROM kafka_effects").Scan(&count); err != nil || count != 0 {
			t.Fatal("uncommitted/failed effect visible", err)
		}
	}
	assertNotCommitted()
	stopFirst()
	select {
	case err = <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("first consumer failed bounded shutdown")
	}
	assertNotCommitted()

	// A different group member now owns the specific failed partition, starts
	// from the broker's last committed offset and holds the retried transaction
	// open. A local success notification is not treated as commit evidence.
	assigned := make(chan struct{}, 1)
	secondOpts := append(append([]kgo.Opt{}, consumerOpts...), kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
		for _, p := range partitions["identity.events.v2"] {
			if p == firstRecord.Partition {
				select {
				case assigned <- struct{}{}:
				default:
				}
			}
		}
	}))
	secondClient, err := kgo.NewClient(secondOpts...)
	if err != nil {
		t.Fatal(err)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	defer stopSecond()
	retried := make(chan struct{})
	release := make(chan struct{})
	secondConsumer := eventConsumer{db: db, key: make([]byte, 32), apply: func(ctx context.Context, tx pgx.Tx, e eventsv2.Envelope) error {
		if e.AggregateID != aggregate {
			return nil
		}
		if _, err := tx.Exec(ctx, "INSERT INTO kafka_effects VALUES($1)", e.EventID); err != nil {
			return err
		}
		if e.EventID == first.EventID {
			close(retried)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}}
	secondDone := make(chan error, 1)
	go func() { secondDone <- secondConsumer.run(secondCtx, secondClient) }()
	select {
	case <-assigned:
	case <-ctx.Done():
		t.Fatal("failed partition was not assigned to replacement member")
	}
	select {
	case <-retried:
	case <-ctx.Done():
		t.Fatal("replacement skipped failed transaction")
	}
	assertNotCommitted()
	close(release)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		offset, err := brokerCommittedOffset(ctx, producer, group, "identity.events.v2", firstRecord.Partition)
		if err != nil {
			t.Fatal(err)
		}
		if offset >= secondRecord.Offset+1 {
			var effects, receipts int
			err = db.QueryRow(ctx, "SELECT (SELECT count(*) FROM kafka_effects),(SELECT count(*) FROM inbox WHERE event_id=$1 OR event_id=$2)", first.EventID, second.EventID).Scan(&effects, &receipts)
			if err != nil {
				t.Fatal(err)
			}
			if effects != 2 || receipts != 2 {
				t.Fatal("broker committed before durable effects and inbox")
			}
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("replacement did not commit durable transaction then broker offset")
		}
	}
	stopSecond()
	select {
	case err = <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("replacement consumer failed bounded shutdown")
	}
}
