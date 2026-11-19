-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Node activity finds a node's older packets heard again lately in one range, by their last hearing.
CREATE INDEX CONCURRENTLY idx_packets_origin_last_heard ON packets USING btree (origin_pubkey, last_heard_at DESC) WHERE (origin_pubkey IS NOT NULL);
