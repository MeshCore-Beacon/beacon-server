-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Concurrent upserts can leave last_heard_at below first_heard_at; window pages read those rows here.
CREATE INDEX CONCURRENTLY idx_packets_heard_inverted ON packets USING btree (first_heard_at) INCLUDE (last_heard_at, packet_hash, payload_type, route_type, scope_id) WHERE ((first_heard_at > last_heard_at) IS TRUE);
