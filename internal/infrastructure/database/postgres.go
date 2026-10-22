package database

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"user-notification-service/internal/domain"
)

type PostgresRepo struct {
	pool *pgxpool.Pool
}

func NewPostgresRepo(ctx context.Context, databaseURL string) (*PostgresRepo, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres config: %w", err)
	}

	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnLifetime = time.Hour

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres ping failed: %w", err)
	}

	return &PostgresRepo{pool: pool}, nil
}

func (r *PostgresRepo) Close() {
	r.pool.Close()
}

func (r *PostgresRepo) CreateProfileWithDefaults(ctx context.Context, userID uuid.UUID, email string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 1. Insert user profile
	profileQuery := `
		INSERT INTO user_profiles (user_id, email, created_at, updated_at)
		VALUES ($1, $2, NOW(), NOW())
		ON CONFLICT (user_id) DO NOTHING;
	`
	if _, err := tx.Exec(ctx, profileQuery, userID, email); err != nil {
		return fmt.Errorf("failed to insert user profile: %w", err)
	}

	// 2. Insert default notification settings
	settingsQuery := `
		INSERT INTO notification_settings (
			user_id, email_login_alerts, email_security_alerts, email_news_digests,
			telegram_login_alerts, telegram_security_alerts, created_at, updated_at
		)
		VALUES ($1, true, true, false, false, false, NOW(), NOW())
		ON CONFLICT (user_id) DO NOTHING;
	`
	if _, err := tx.Exec(ctx, settingsQuery, userID); err != nil {
		return fmt.Errorf("failed to insert notification settings: %w", err)
	}

	return tx.Commit(ctx)
}

func (r *PostgresRepo) GetProfile(ctx context.Context, userID uuid.UUID) (*domain.UserProfile, error) {
	query := `
		SELECT user_id, email, display_name, avatar_url, phone_number, telegram_id, created_at, updated_at
		FROM user_profiles
		WHERE user_id = $1;
	`
	row := r.pool.QueryRow(ctx, query, userID)

	var p domain.UserProfile
	err := row.Scan(
		&p.UserID, &p.Email, &p.DisplayName, &p.AvatarURL,
		&p.PhoneNumber, &p.TelegramID, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &p, nil
}

func (r *PostgresRepo) UpdateProfile(ctx context.Context, p *domain.UserProfile) error {
	query := `
		UPDATE user_profiles
		SET display_name = $1, avatar_url = $2, phone_number = $3, telegram_id = $4, updated_at = NOW()
		WHERE user_id = $5;
	`
	_, err := r.pool.Exec(ctx, query, p.DisplayName, p.AvatarURL, p.PhoneNumber, p.TelegramID, p.UserID)
	return err
}

func (r *PostgresRepo) GetNotificationSettings(ctx context.Context, userID uuid.UUID) (*domain.NotificationSettings, error) {
	query := `
		SELECT user_id, email_login_alerts, email_security_alerts, email_news_digests,
		       telegram_login_alerts, telegram_security_alerts, created_at, updated_at
		FROM notification_settings
		WHERE user_id = $1;
	`
	row := r.pool.QueryRow(ctx, query, userID)

	var s domain.NotificationSettings
	err := row.Scan(
		&s.UserID, &s.EmailLoginAlerts, &s.EmailSecurityAlerts, &s.EmailNewsDigests,
		&s.TelegramLoginAlerts, &s.TelegramSecurityAlerts, &s.CreatedAt, &s.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &s, nil
}

func (r *PostgresRepo) UpdateNotificationSettings(ctx context.Context, s *domain.NotificationSettings) error {
	query := `
		UPDATE notification_settings
		SET email_login_alerts = $1, email_security_alerts = $2, email_news_digests = $3,
		    telegram_login_alerts = $4, telegram_security_alerts = $5, updated_at = NOW()
		WHERE user_id = $6;
	`
	_, err := r.pool.Exec(
		ctx, query,
		s.EmailLoginAlerts, s.EmailSecurityAlerts, s.EmailNewsDigests,
		s.TelegramLoginAlerts, s.TelegramSecurityAlerts, s.UserID,
	)
	return err
}

func (r *PostgresRepo) IsEventProcessed(ctx context.Context, eventID uuid.UUID) (bool, error) {
	query := `SELECT 1 FROM processed_events WHERE event_id = $1 LIMIT 1;`
	var dummy int
	err := r.pool.QueryRow(ctx, query, eventID).Scan(&dummy)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (r *PostgresRepo) MarkEventProcessed(ctx context.Context, eventID uuid.UUID, eventType string) error {
	query := `
		INSERT INTO processed_events (event_id, event_type, processed_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (event_id) DO NOTHING;
	`
	_, err := r.pool.Exec(ctx, query, eventID, eventType)
	return err
}
