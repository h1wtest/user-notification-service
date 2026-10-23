package platformkit

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func kafkaOptions(c Config) ([]kgo.Opt, error) {
	if len(c.Brokers) == 0 || c.KafkaUsername == "" || c.KafkaPassword == "" {
		return nil, errors.New("Kafka TLS/SCRAM credentials and brokers required")
	}
	pem, err := os.ReadFile(c.KafkaCA)
	if err != nil {
		return nil, errors.New("Kafka CA required")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("invalid Kafka CA")
	}
	// Ambiguous broker outcomes must return before the durable 30-second lease
	// expires. Default idempotency ignores cancellation once a write is unsure;
	// explicit cancellation keeps internal dedupe but permits application retries
	// to duplicate an already-written record. Transactional inbox handles that.
	return []kgo.Opt{kgo.SeedBrokers(c.Brokers...), kgo.ClientID(c.Name), kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}), kgo.DialTimeout(3 * time.Second), kgo.SASL(scram.Auth{User: c.KafkaUsername, Pass: c.KafkaPassword}.AsSha256Mechanism()), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.AllowIdempotentProduceCancellation(), kgo.ProducerBatchMaxBytes(bodyLimit + 64<<10), kgo.RecordDeliveryTimeout(15 * time.Second), kgo.ProduceRequestTimeout(3 * time.Second), kgo.RequestTimeoutOverhead(2 * time.Second)}, nil
}
func pause(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func retryDelay(attempt int) time.Duration {
	return time.Duration(1<<min(max(attempt-1, 0), 8)) * time.Second
}
