// Package platformkit contains technical adapters only. Business state and owner
// migrations belong to the importing service.
package platformkit

import (
	"errors"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Name, DSN, Port, Origin, Token, IdentityURL, Cert, Key, CA string
	KafkaCA, KafkaUsername, KafkaPassword                      string
	Brokers                                                    []string
}

func Load(name, defaultPort string) (Config, error) {
	c := Config{Name: name, DSN: os.Getenv("DATABASE_URL"), Port: env("HTTP_PORT", defaultPort), Origin: os.Getenv("PUBLIC_ORIGIN"), Token: os.Getenv("INTERNAL_SERVICE_TOKEN"), IdentityURL: env("IDENTITY_URL", "https://identity-service:8001"), Cert: os.Getenv("TLS_CERT_FILE"), Key: os.Getenv("TLS_KEY_FILE"), CA: os.Getenv("INTERNAL_CA_FILE"), KafkaCA: os.Getenv("KAFKA_TLS_CA_FILE"), KafkaUsername: os.Getenv("KAFKA_SASL_USERNAME"), KafkaPassword: os.Getenv("KAFKA_SASL_PASSWORD")}
	for _, broker := range strings.Split(os.Getenv("KAFKA_BROKERS"), ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			c.Brokers = append(c.Brokers, broker)
		}
	}
	port, err := strconv.Atoi(c.Port)
	if name == "" || strings.TrimSpace(name) != name || c.DSN == "" || err != nil || port < 1 || port > 65535 {
		return c, errors.New("service name, DATABASE_URL and valid HTTP_PORT required")
	}
	if c.Token == "" || c.Cert == "" || c.Key == "" || c.CA == "" {
		return c, errors.New("service token and TLS certificate/key/CA required")
	}
	if !httpsURL(c.IdentityURL) || !httpsURL(c.Origin) {
		return c, errors.New("HTTPS identity URL and public origin required")
	}
	u, _ := url.Parse(c.Origin)
	if u.Path != "" && u.Path != "/" || u.RawQuery != "" || u.Fragment != "" {
		return c, errors.New("PUBLIC_ORIGIN must be an origin")
	}
	if len(c.Brokers) == 0 || c.KafkaCA == "" || c.KafkaUsername == "" || c.KafkaPassword == "" {
		return c, errors.New("Kafka brokers, TLS CA and SASL credentials required")
	}
	for _, broker := range c.Brokers {
		if _, _, err := net.SplitHostPort(broker); err != nil {
			return c, errors.New("invalid Kafka broker address")
		}
	}
	return c, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func httpsURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.Fragment == ""
}
