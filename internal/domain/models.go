package domain

import (
	"time"

	"github.com/google/uuid"
)

type UserProfile struct {
	UserID      uuid.UUID `json:"user_id"`
	Email       string    `json:"email"`
	DisplayName *string   `json:"display_name"`
	AvatarURL   *string   `json:"avatar_url"`
	PhoneNumber *string   `json:"phone_number"`
	TelegramID  *string   `json:"telegram_id"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type NotificationSettings struct {
	UserID                 uuid.UUID `json:"user_id"`
	EmailLoginAlerts       bool      `json:"email_login_alerts"`
	EmailSecurityAlerts    bool      `json:"email_security_alerts"`
	EmailNewsDigests       bool      `json:"email_news_digests"`
	TelegramLoginAlerts    bool      `json:"telegram_login_alerts"`
	TelegramSecurityAlerts bool      `json:"telegram_security_alerts"`
	CreatedAt              time.Time `json:"created_at"`
	UpdatedAt              time.Time `json:"updated_at"`
}
