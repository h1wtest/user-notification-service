package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"user-notification-service/internal/infrastructure/database"
	"user-notification-service/internal/infrastructure/mailer"
)

type EventEnvelope struct {
	EventID        string          `json:"event_id"`
	EventType      string          `json:"event_type"`
	Producer       string          `json:"producer"`
	Timestamp      string          `json:"timestamp"`
	UserRegistered *UserRegistered `json:"user_registered,omitempty"`
	UserLoggedIn   *UserLoggedIn   `json:"user_logged_in,omitempty"`
}

type UserRegistered struct {
	UserID           string `json:"user_id"`
	Email            string `json:"email"`
	VerificationCode string `json:"verification_code"`
	ExpiresAt        string `json:"expires_at"`
}

type UserLoggedIn struct {
	UserID       string `json:"user_id"`
	SessionID    string `json:"session_id"`
	IPAddress    string `json:"ip_address"`
	UserAgent    string `json:"user_agent"`
	DeviceName   string `json:"device_name"`
	Location     string `json:"location"`
	IsNewDevice  bool   `json:"is_new_device"`
	IsSuspicious bool   `json:"is_suspicious"`
	LoggedInAt   string `json:"logged_in_at"`
}

type EventConsumer struct {
	client *kgo.Client
	repo   *database.PostgresRepo
	mailer *mailer.Mailer
	logger *slog.Logger
	topic  string
}

func NewEventConsumer(
	brokers []string,
	group string,
	topic string,
	repo *database.PostgresRepo,
	mailer *mailer.Mailer,
	logger *slog.Logger,
) (*EventConsumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.BlockRebalanceOnPoll(),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &EventConsumer{
		client: client,
		repo:   repo,
		mailer: mailer,
		logger: logger,
		topic:  topic,
	}, nil
}

func (c *EventConsumer) Start(ctx context.Context) error {
	c.logger.Info("Kafka Event Consumer started", "topic", c.topic)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Stopping Kafka Event Consumer...")
			c.client.Close()
			return nil
		default:
			fetches := c.client.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, err := range errs {
					c.logger.Error("Kafka fetch error", "err", err.Err, "topic", err.Topic)
				}
				time.Sleep(500 * time.Millisecond)
				continue
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				if err := c.handleRecord(ctx, record); err != nil {
					c.logger.Error("Failed to process Kafka record", "err", err, "offset", record.Offset)
				}
			}
		}
	}
}

func (c *EventConsumer) handleRecord(ctx context.Context, record *kgo.Record) error {
	var env EventEnvelope
	if err := json.Unmarshal(record.Value, &env); err != nil {
		c.logger.Warn("Failed to unmarshal event envelope, skipping", "err", err)
		return nil
	}

	eventID, err := uuid.Parse(env.EventID)
	if err != nil {
		c.logger.Warn("Invalid event_id UUID, skipping", "event_id", env.EventID)
		return nil
	}

	// 1. Idempotency Check: check if event was already handled
	alreadyProcessed, err := c.repo.IsEventProcessed(ctx, eventID)
	if err != nil {
		return fmt.Errorf("idempotency check error: %w", err)
	}
	if alreadyProcessed {
		c.logger.Debug("Event already processed, skipping", "event_id", eventID, "type", env.EventType)
		return nil
	}

	c.logger.Info("Processing event", "type", env.EventType, "event_id", eventID)

	switch env.EventType {
	case "auth.user_registered":
		if env.UserRegistered == nil {
			return fmt.Errorf("missing user_registered payload")
		}
		userID, err := uuid.Parse(env.UserRegistered.UserID)
		if err != nil {
			return fmt.Errorf("invalid user_id: %w", err)
		}

		// Create user profile and default notification settings
		if err := c.repo.CreateProfileWithDefaults(ctx, userID, env.UserRegistered.Email); err != nil {
			return fmt.Errorf("failed to create default user profile: %w", err)
		}

		// Send verification email via Mailpit SMTP
		if err := c.mailer.SendVerificationEmail(env.UserRegistered.Email, env.UserRegistered.VerificationCode); err != nil {
			c.logger.Error("Failed to send verification email", "email", env.UserRegistered.Email, "err", err)
			return err
		}
		c.logger.Info("Verification email sent successfully", "email", env.UserRegistered.Email)

	case "auth.user_logged_in":
		if env.UserLoggedIn == nil {
			return fmt.Errorf("missing user_logged_in payload")
		}
		userID, err := uuid.Parse(env.UserLoggedIn.UserID)
		if err != nil {
			return fmt.Errorf("invalid user_id: %w", err)
		}

		// Check user notification settings
		settings, err := c.repo.GetNotificationSettings(ctx, userID)
		if err != nil {
			return fmt.Errorf("failed to fetch notification settings: %w", err)
		}

		if settings != nil && settings.EmailLoginAlerts {
			profile, err := c.repo.GetProfile(ctx, userID)
			if err != nil || profile == nil {
				return fmt.Errorf("failed to fetch user profile for alert: %w", err)
			}

			loginTime, _ := time.Parse(time.RFC3339, env.UserLoggedIn.LoggedInAt)
			if loginTime.IsZero() {
				loginTime = time.Now()
			}

			if err := c.mailer.SendLoginAlertEmail(
				profile.Email,
				env.UserLoggedIn.IPAddress,
				env.UserLoggedIn.UserAgent,
				env.UserLoggedIn.DeviceName,
				loginTime,
			); err != nil {
				c.logger.Error("Failed to send login alert email", "email", profile.Email, "err", err)
				return err
			}
			c.logger.Info("Login alert email sent successfully", "email", profile.Email)
		}
	}

	// 2. Mark event processed for idempotency
	return c.repo.MarkEventProcessed(ctx, eventID, env.EventType)
}
