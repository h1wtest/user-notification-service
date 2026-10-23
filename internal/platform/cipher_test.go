package platform

import (
	"encoding/base64"
	"testing"
)

func TestDecryptIndependentOpenSSLEventFixture(t *testing.T) {
	key, err := base64.StdEncoding.DecodeString("ERERERERERERERERERERERERERERERERERERERERERE=")
	if err != nil {
		t.Fatal(err)
	}
	const encrypted = "IiIiIiIiIiIiIiIibNVyOqW9wDaBHeQeeJfQtC+j/dJHsZx7YmI92DnRSDt1T5svY5jJ7n5o3R3WaHkHkf68B/gTE5KWTsifixObxwrpXF0p5DxihwQY+Hmfn2CM2YESIY20Nzwj64gPkDLgvXdxMlsy8z417ohkKzrrB1k72JdiGeoMsxZVHUXeJuJ0qNrPqVsKDldIyCTy/6F2vVZ5EUWFOXs="
	got, err := Decrypt(key, "019a0000-0000-7000-8000-000000000001", encrypted)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"user_id":"019a0000-0000-7000-8000-000000000002","email":"fixture@example.invalid","code":"123456","expires_at":"2026-10-23T12:00:00Z"}`
	if string(got) != want {
		t.Fatalf("wire mismatch: %s", got)
	}
	if _, err = Decrypt(key, "019a0000-0000-7000-8000-000000000002", encrypted); err == nil {
		t.Fatal("accepted modified event identity")
	}
}
