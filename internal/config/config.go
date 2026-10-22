package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	HTTPPort           int
	DatabaseURL        string
	RedisURL           string
	KafkaBrokers       []string
	KafkaConsumerGroup string
	KafkaTopic         string
	SMTPHost           string
	SMTPPort           int
	SMTPFrom           string
	JWTSecret          string
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	httpPort := getEnvAsInt("HTTP_PORT", 8002)
	dbURL := getEnv("DATABASE_URL", "postgres://postgres:postgres_secret_pass@localhost:5432/user_db?sslmode=disable")
	redisURL := getEnv("REDIS_URL", "redis://:redis_secret_pass@localhost:6379/1")

	brokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	brokers := strings.Split(brokersStr, ",")
	for i, b := range brokers {
		brokers[i] = strings.TrimSpace(b)
	}

	consumerGroup := getEnv("KAFKA_CONSUMER_GROUP", "user-notification-service-group")
	topic := getEnv("KAFKA_TOPIC", "identity.events")

	smtpHost := getEnv("SMTP_HOST", "localhost")
	smtpPort := getEnvAsInt("SMTP_PORT", 1025)
	smtpFrom := getEnv("SMTP_FROM", "no-reply@our-sso.local")
	jwtSecret := getEnv("JWT_SECRET", "super_secret_jwt_hmac_key_min_32_bytes_long_here_12345")

	if jwtSecret == "" {
		return nil, fmt.Errorf("JWT_SECRET must not be empty")
	}

	return &Config{
		HTTPPort:           httpPort,
		DatabaseURL:        dbURL,
		RedisURL:           redisURL,
		KafkaBrokers:       brokers,
		KafkaConsumerGroup: consumerGroup,
		KafkaTopic:         topic,
		SMTPHost:           smtpHost,
		SMTPPort:           smtpPort,
		SMTPFrom:           smtpFrom,
		JWTSecret:          jwtSecret,
	}, nil
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func getEnvAsInt(key string, defaultVal int) int {
	valStr := getEnv(key, "")
	if val, err := strconv.Atoi(valStr); err == nil {
		return val
	}
	return defaultVal
}
