package notification

import (
	"context"
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestReminderSchedulesAreIdempotentAndOldVersionsCancel(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	id, user := uuid.NewString(), uuid.NewString()
	end := time.Now().UTC().Add(10 * 24 * time.Hour)
	add := func(version int64, end time.Time) {
		t.Helper()
		tx, err := db.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if err = insertReminderPlans(ctx, tx, &reminderPlan{SubscriptionID: id, UserID: user, State: "ACTIVE", Version: version, PeriodEnd: end}); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	add(1, end)
	add(1, end)
	add(2, end.Add(24*time.Hour))
	var pending, cancelled int
	if err := db.QueryRow(ctx, "SELECT count(*) FILTER(WHERE state='pending'),count(*) FILTER(WHERE state='cancelled') FROM notification_reminders WHERE subscription_id=$1", id).Scan(&pending, &cancelled); err != nil || pending != 2 || cancelled != 2 {
		t.Fatalf("schedule duplication/stale version %d %d %v", pending, cancelled, err)
	}
}
func TestReminderRequiresExactCurrentVersionAndEnd(t *testing.T) {
	end := time.Now().UTC().Add(time.Hour)
	r := scheduledReminder{UserID: "user", SubscriptionID: "subscription", Version: 2, PeriodEnd: end}
	current := subscriptionState{ID: "subscription", UserID: "user", Version: 3, State: "ACTIVE", PeriodEnd: end}
	if reminderCurrent(r, current, time.Now()) {
		t.Fatal("stale reminder accepted")
	}
	current.Version = 2
	if !reminderCurrent(r, current, time.Now()) {
		t.Fatal("current reminder rejected")
	}
	current.PeriodEnd = end.Add(time.Second)
	if reminderCurrent(r, current, time.Now()) {
		t.Fatal("changed end accepted")
	}
}
