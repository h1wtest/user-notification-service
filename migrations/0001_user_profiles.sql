-- =============================================================
-- User Profiles table
-- =============================================================
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id UUID PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    display_name VARCHAR(100),
    avatar_url TEXT,
    phone_number VARCHAR(30),
    telegram_id VARCHAR(50),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_profiles_email ON user_profiles (email);

-- =============================================================
-- Notification Settings table
-- =============================================================
CREATE TABLE IF NOT EXISTS notification_settings (
    user_id UUID PRIMARY KEY REFERENCES user_profiles(user_id) ON DELETE CASCADE,
    email_login_alerts BOOLEAN NOT NULL DEFAULT TRUE,
    email_security_alerts BOOLEAN NOT NULL DEFAULT TRUE,
    email_news_digests BOOLEAN NOT NULL DEFAULT FALSE,
    telegram_login_alerts BOOLEAN NOT NULL DEFAULT FALSE,
    telegram_security_alerts BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================
-- Processed Events table (Idempotent Consumer Pattern)
-- =============================================================
CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_processed_events_type ON processed_events (event_type);
