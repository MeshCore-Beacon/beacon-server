-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Packet pages walk (last_heard_at, packet_hash); the hash breaks millisecond ties.
CREATE INDEX CONCURRENTLY idx_packets_last_heard_hash ON packets USING btree (last_heard_at DESC, packet_hash DESC);
