// SPDX-License-Identifier: MIT
package notification

import (
	"encoding/base64"
	"errors"
	"github.com/google/uuid"
	"strings"
	"time"
)

func parseDeliveryCursor(value string) (time.Time, string, error) {
	if value == "" || len(value) > 256 {
		return time.Time{}, "", errors.New("invalid_cursor")
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return time.Time{}, "", errors.New("invalid_cursor")
	}
	timestamp, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return time.Time{}, "", errors.New("invalid_cursor")
	}
	created, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil || created.IsZero() {
		return time.Time{}, "", errors.New("invalid_cursor")
	}
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id {
		return time.Time{}, "", errors.New("invalid_cursor")
	}
	return created.UTC(), id, nil
}

func encodeDeliveryCursor(created time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(created.UTC().Format(time.RFC3339Nano) + "|" + id))
}
