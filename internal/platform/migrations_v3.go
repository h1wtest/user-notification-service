// SPDX-License-Identifier: MIT
package platform

// Applied version2 bytes remain untouched; semantic identity-admin metadata is
// an additive owner-only upgrade and never stores raw owner/provider payloads.
const auditAdminActionSQL = `
ALTER TABLE audit_events ADD COLUMN operation text;
ALTER TABLE audit_events ADD COLUMN target_type text;
ALTER TABLE audit_events ADD COLUMN target_id text;
ALTER TABLE audit_events ADD COLUMN reason text;
ALTER TABLE audit_events ADD CONSTRAINT audit_events_action_target_v3 CHECK(target_type IS NULL OR target_type IN('role','user','oauth_client'));
ALTER TABLE audit_events ADD CONSTRAINT audit_events_action_target_length_v3 CHECK(target_id IS NULL OR octet_length(target_id) BETWEEN 1 AND 400);
ALTER TABLE audit_events ADD CONSTRAINT audit_events_action_reason_v3 CHECK(reason IS NULL OR octet_length(reason) BETWEEN 1 AND 2000);
`

func ownerSchemaVersion(owner string) int {
	if owner == "admin" {
		return 4
	}
	return 3
}
