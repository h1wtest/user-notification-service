package profile

import (
	"strings"
	"testing"
)

func TestProfilePolicyRejectsUnsafeAvatarOutsideHTTP(t *testing.T) {
	bad := "javascript:alert(1)"
	if (ProfileChanges{Avatar: NullableString{Present: true, Value: &bad}}).Validate() == nil {
		t.Fatal("unsafe avatar accepted by application policy")
	}
	valid := "https://example.test/avatar.png"
	if err := (ProfileChanges{Avatar: NullableString{Present: true, Value: &valid}}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestProfilePolicyAcceptsNullEmptyAndLengthBoundaries(t *testing.T) {
	for _, tc := range []struct{ name, value string }{
		{"empty", ""},
		{"maximum display", strings.Repeat("d", 100)},
		{"maximum phone", strings.Repeat("1", 30)},
		{"maximum avatar", "https://example.test/" + strings.Repeat("a", 2027)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			field := NullableString{Present: true, Value: &tc.value}
			var changes ProfileChanges
			switch tc.name {
			case "empty":
				changes = ProfileChanges{Display: field, Avatar: field, Phone: field}
			case "maximum display":
				changes.Display = field
			case "maximum phone":
				changes.Phone = field
			case "maximum avatar":
				changes.Avatar = field
			}
			if err := changes.Validate(); err != nil {
				t.Fatalf("valid profile rejected: %v", err)
			}
		})
	}
	clear := NullableString{Present: true}
	if err := (ProfileChanges{Display: clear, Avatar: clear, Phone: clear}).Validate(); err != nil {
		t.Fatalf("explicit null rejected: %v", err)
	}
}
