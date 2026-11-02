-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Typed pages read one range per (payload type, route type) pair.
CREATE INDEX CONCURRENTLY idx_packets_type_last_heard ON packets USING btree (payload_type, route_type, last_heard_at DESC, packet_hash DESC);
