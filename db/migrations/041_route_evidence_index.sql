-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- One statement outside a transaction. RunMigrations recovers interrupted builds;
-- avoid IF NOT EXISTS, which would silently accept an invalid index.
-- A compact digest narrows candidates; queries still compare the complete bytes.
CREATE INDEX CONCURRENTLY idx_observations_route_evidence
ON packet_observations (iata, hash_size, (decode(md5(path_bytes), 'hex')), heard_at DESC, id DESC)
WHERE path_bytes IS NOT NULL AND payload_type IS NOT NULL AND payload_type <> 9 AND hop_count >= 2;
