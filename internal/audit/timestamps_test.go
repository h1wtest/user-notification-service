// SPDX-License-Identifier: MIT
package audit

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// pgx.Row is the database decoding boundary. A non-UTC location deliberately
// models a scanned timestamptz without depending on the runner's host timezone.
type localCommandRow struct{ at time.Time }

func (row localCommandRow) Scan(dest ...any) error {
	*dest[0].(*string) = testCommand
	*dest[1].(*string) = testActor
	*dest[2].(*string) = "ban"
	*dest[3].(*string) = testTarget
	*dest[4].(*string) = "timezone case"
	*dest[5].(*string) = "timezone-key"
	*dest[6].(*string) = "requested"
	*dest[7].(*json.RawMessage) = nil
	*dest[8].(*time.Time) = row.at
	*dest[9].(*time.Time) = row.at
	return nil
}

func TestScannedLocalZoneCommandSerializesCanonicalUTC(t *testing.T) {
	local := time.Date(2026, 10, 24, 15, 0, 0, 0, time.FixedZone("database-local", 3*60*60))
	command, err := scanCommand(localCommandRow{at: local})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"created_at":"2026-10-24T12:00:00Z"`) || !strings.Contains(string(raw), `"updated_at":"2026-10-24T12:00:00Z"`) {
		t.Fatalf("scanned local-zone timestamps were not canonical: %s", raw)
	}
}
