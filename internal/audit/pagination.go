// SPDX-License-Identifier: MIT
package audit

import (
	"encoding/base64"
	"errors"
	"github.com/google/uuid"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Page struct {
	Limit    int
	Before   time.Time
	BeforeID string
}

func parsePage(r *http.Request) (Page, error) {
	page := Page{Limit: 50}
	if values, exists := r.URL.Query()["limit"]; exists {
		if len(values) != 1 {
			return page, errors.New("invalid_limit")
		}
		n, err := strconv.Atoi(values[0])
		if err != nil || n < 1 || n > 100 {
			return page, errors.New("invalid_limit")
		}
		page.Limit = n
	}
	values, exists := r.URL.Query()["cursor"]
	if !exists {
		return page, nil
	}
	if len(values) != 1 || len(values[0]) > 256 {
		return page, errors.New("invalid_cursor")
	}
	raw, err := base64.RawURLEncoding.DecodeString(values[0])
	if err != nil {
		return page, errors.New("invalid_cursor")
	}
	timestamp, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return page, errors.New("invalid_cursor")
	}
	page.Before, err = time.Parse(time.RFC3339Nano, timestamp)
	if err != nil || page.Before.IsZero() {
		return page, errors.New("invalid_cursor")
	}
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id {
		return page, errors.New("invalid_cursor")
	}
	page.BeforeID = id
	return page, nil
}
func encodeCursor(created time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(created.UTC().Format(time.RFC3339Nano) + "|" + id))
}
