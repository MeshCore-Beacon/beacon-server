-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- RunMigrations executes each file outside an explicit transaction. Keep this
-- as one statement so other subscribers can keep writing during the build.
-- If interrupted, check pg_index.indisvalid and remove this named index before
-- retrying; do not silently accept an invalid index with IF NOT EXISTS.
CREATE INDEX CONCURRENTLY idx_known_routes_iata_last_seen
ON known_routes (iata, last_seen DESC);
