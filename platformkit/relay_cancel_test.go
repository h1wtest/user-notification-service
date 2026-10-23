package platformkit

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

// blackholeProduceConn preserves the real TLS/SCRAM/metadata exchange and sends
// Produce requests to the real broker, but discards every Produce response. The
// application therefore cannot distinguish a lost acknowledgment from no write.
// kafkaOptions supplies the actual socket deadlines; this wrapper adds none.
// Pinned franz writes each complete Kafka frame through a single Write call.
// Once a connection sees Produce, all subsequent responses on it are discarded.
// The bounded producer must fail that ambiguous operation before its DB lease.
type blackholeProduceConn struct {
	net.Conn
	drop    atomic.Bool
	written chan struct{}
	once    *sync.Once
}

func (c *blackholeProduceConn) Write(raw []byte) (int, error) {
	produce := len(raw) >= 8 && int(binary.BigEndian.Uint32(raw[:4])) == len(raw)-4 && binary.BigEndian.Uint16(raw[4:6]) == 0
	if produce {
		c.drop.Store(true)
	}
	n, err := c.Conn.Write(raw)
	if produce && n == len(raw) && err == nil {
		c.once.Do(func() { close(c.written) })
	}
	return n, err
}
func (c *blackholeProduceConn) Read(raw []byte) (int, error) {
	for {
		n, err := c.Conn.Read(raw)
		if err != nil || !c.drop.Load() {
			return n, err
		}
	}
}

func TestAmbiguousBrokerCancellationReturnsBeforeLeaseAndKeepsOutboxPending(t *testing.T) {
	config := kafkaFixture(t)
	db := migratedDB(t)
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	e := fixtureEvent(t, uuid.NewString(), 1)
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = Enqueue(ctx, tx, e); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	leased, err := leaseOutbox(ctx, db, "identity")
	if err != nil {
		t.Fatal(err)
	}
	opts, err := kafkaOptions(config)
	if err != nil {
		t.Fatal(err)
	}
	pem, err := os.ReadFile(config.KafkaCA)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		t.Fatal("invalid broker test CA")
	}
	dialer := tls.Dialer{NetDialer: &net.Dialer{Timeout: 3 * time.Second}, Config: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}
	written := make(chan struct{})
	var once sync.Once
	// Override only the dial implementation; retain the production timeout,
	// acknowledgment and idempotent cancellation options under test.
	opts = append(opts, kgo.DialTLSConfig(nil), kgo.Dialer(func(ctx context.Context, network, host string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, host)
		if err != nil {
			return nil, err
		}
		return &blackholeProduceConn{Conn: conn, written: written, once: &once}, nil
	}))
	client, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	attempt, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- client.ProduceSync(attempt, &kgo.Record{Topic: leased.Topic, Key: []byte(leased.Key), Value: leased.Raw}).FirstErr()
	}()
	select {
	case <-written:
	case <-ctx.Done():
		t.Fatal("real Produce request not written")
	}
	cancel()
	var publishErr error
	select {
	case publishErr = <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("ambiguous in-flight publish ignored cancellation beyond socket deadline")
	}
	if publishErr == nil {
		t.Fatal("discarded acknowledgment reported successful delivery")
	}
	if !errors.Is(publishErr, context.Canceled) {
		t.Fatalf("ambiguous cancellation returned %v", publishErr)
	}
	if err = finishOutbox(ctx, db, leased, publishErr); err != nil {
		t.Fatal("publication did not return before fenced lease expired", err)
	}
	var acknowledged bool
	var reason string
	if err = db.QueryRow(ctx, "SELECT published_at IS NOT NULL,last_error FROM outbox WHERE event_id=$1", e.EventID).Scan(&acknowledged, &reason); err != nil {
		t.Fatal(err)
	}
	if acknowledged || reason != "broker_unavailable" {
		t.Fatal("ambiguous outcome lost durable pending retry")
	}
	closed := make(chan struct{})
	go func() { client.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("producer shutdown remained blocked after cancellation")
	}
}
