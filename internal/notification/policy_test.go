package notification

import (
	"testing"
)

func TestPolicyStopsOldTelegramBindingAndRevokedStaff(t *testing.T) {
	chat := "123"
	p := recipientPolicy{TelegramChatID: &chat, BindingVersion: 4, TelegramSecurity: true}
	b := Body{To: chat, Category: "security", BindingVersion: 3}
	if policyAllows("telegram", b, p, eligibility{Active: true}) {
		t.Fatal("old binding redirected")
	}
	b.BindingVersion = 4
	b.Category = "admin"
	b.RequiredPermission = "billing:resolve"
	if policyAllows("telegram", b, p, eligibility{Active: true, Staff: false, Permissions: []string{"billing:resolve"}}) {
		t.Fatal("revoked staff accepted")
	}
	if !policyAllows("telegram", b, p, eligibility{Active: true, Staff: true, Permissions: []string{"billing:resolve"}}) {
		t.Fatal("current authorized staff denied")
	}
}
func TestPolicyTransactionalEmailIgnoresOptionalPreference(t *testing.T) {
	p := recipientPolicy{Email: "user@example.test"}
	e := eligibility{Active: true, Email: p.Email}
	if !policyAllows("email", Body{To: p.Email, Category: "transactional"}, p, e) {
		t.Fatal("transactional disabled")
	}
	if policyAllows("email", Body{To: p.Email, Category: "subscription"}, p, e) {
		t.Fatal("optional channel enabled")
	}
	e.Banned = true
	if policyAllows("email", Body{To: p.Email, Category: "security"}, p, e) {
		t.Fatal("banned user accepted")
	}
}
