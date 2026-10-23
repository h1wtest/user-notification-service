package platformkit

import "testing"

func TestLoadRejectsMissingIdentityAndPlaintextDependencies(t *testing.T) {
	values := map[string]string{"DATABASE_URL": "postgres://owner@localhost/db", "HTTP_PORT": "8006", "PUBLIC_ORIGIN": "https://localhost:8448", "INTERNAL_SERVICE_TOKEN": "workload", "IDENTITY_URL": "https://identity-service:8001", "TLS_CERT_FILE": "cert.pem", "TLS_KEY_FILE": "key.pem", "INTERNAL_CA_FILE": "ca.pem", "KAFKA_BROKERS": " broker:9092,other:9092 ", "KAFKA_TLS_CA_FILE": "kafka.pem", "KAFKA_SASL_USERNAME": "catalog", "KAFKA_SASL_PASSWORD": "secret"}
	for key, value := range values {
		t.Setenv(key, value)
	}
	c, err := Load("catalog", "8006")
	if err != nil || len(c.Brokers) != 2 || c.Brokers[0] != "broker:9092" {
		t.Fatal("valid config rejected", err)
	}
	for _, key := range []string{"INTERNAL_SERVICE_TOKEN", "DATABASE_URL", "KAFKA_SASL_PASSWORD", "TLS_CERT_FILE"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "")
			if _, err := Load("catalog", "8006"); err == nil {
				t.Fatal("required configuration accepted empty")
			}
		})
	}
	t.Setenv("IDENTITY_URL", "http://identity-service:8001")
	if _, err := Load("catalog", "8006"); err == nil {
		t.Fatal("plaintext identity accepted")
	}
}
