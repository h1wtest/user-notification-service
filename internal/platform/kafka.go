package platform

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func KafkaSecurity(c Config) ([]kgo.Opt, error) {
	pem, err := os.ReadFile(c.KafkaCA)
	if err != nil {
		return nil, errors.New("Kafka CA required")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("invalid Kafka CA")
	}
	if c.KafkaUsername == "" || c.KafkaPassword == "" {
		return nil, errors.New("Kafka SASL credentials required")
	}
	return []kgo.Opt{kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}), kgo.SASL(scram.Auth{User: c.KafkaUsername, Pass: c.KafkaPassword}.AsSha256Mechanism())}, nil
}
