// SPDX-License-Identifier: MIT
package profile

import (
	"context"
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestPreferencesEnableWaitsForUnlinkAndCannotTransferConsent(t *testing.T) {
	db := profilePatchDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	user := uuid.NewString()
	if _, err := db.Exec(ctx, "INSERT INTO profiles(user_id,email,telegram_chat_id,binding_version) VALUES($1,'consent@example.invalid','123',1)", user); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO notification_settings(user_id) VALUES($1)", user); err != nil {
		t.Fatal(err)
	}
	unlink, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer unlink.Rollback(context.Background())
	var locked string
	if err = unlink.QueryRow(ctx, "SELECT user_id FROM profiles WHERE user_id=$1 FOR UPDATE", user).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	var unlinkPID int32
	if err = unlink.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&unlinkPID); err != nil {
		t.Fatal(err)
	}
	if _, err = removeTelegramBinding(ctx, unlink, user); err != nil {
		t.Fatal(err)
	}
	type result struct {
		updated bool
		err     error
	}
	finished := make(chan result, 1)
	go func() {
		updated, err := (&PostgresProfiles{DB: db}).Preferences(ctx, user, Preferences{TS: new(true), TelegramSubscription: new(true), TelegramTrading: new(true)}, true)
		finished <- result{updated, err}
	}()
	// Observe PostgreSQL's actual blocking lock, not a sleep or scheduling guess.
	blocked := false
	for !blocked {
		if err = db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid)) AND query LIKE '%profiles%' AND query NOT LIKE '%pg_stat_activity%')`, unlinkPID).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		select {
		case value := <-finished:
			t.Fatalf("preference update escaped unlink lock: %+v", value)
		default:
		}
		if !blocked {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	if err = unlink.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case value := <-finished:
		if value.err != nil {
			t.Fatal(value.err)
		}
		if value.updated {
			t.Fatal("enabled Telegram without current binding")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// A replacement binding must see only explicit post-binding opt-ins.
	if _, err = db.Exec(ctx, "UPDATE profiles SET telegram_chat_id='456',binding_version=binding_version+1 WHERE user_id=$1", user); err != nil {
		t.Fatal(err)
	}
	var inherited bool
	if err = db.QueryRow(ctx, "SELECT telegram_login_alerts OR telegram_security_alerts OR telegram_subscription OR telegram_trading FROM notification_settings WHERE user_id=$1", user).Scan(&inherited); err != nil {
		t.Fatal(err)
	}
	if inherited {
		t.Fatal("replacement binding inherited previous chat consent")
	}
}
