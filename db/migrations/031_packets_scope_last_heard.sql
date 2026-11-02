-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Scoped pages read one range per scope; the included columns keep typed and windowed scope pages index-only.
CREATE INDEX CONCURRENTLY idx_packets_scope_last_heard ON packets USING btree (scope_id, last_heard_at DESC, packet_hash DESC) INCLUDE (payload_type, route_type, first_heard_at) WHERE (scope_id IS NOT NULL);
