-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- RunMigrations executes this single statement outside an explicit transaction.
-- If interrupted, check pg_index.indisvalid and remove this named index before
-- retrying; do not silently accept an invalid index with IF NOT EXISTS.
CREATE INDEX CONCURRENTLY idx_channels_last_seen_id
ON channels (last_seen DESC, id DESC);
