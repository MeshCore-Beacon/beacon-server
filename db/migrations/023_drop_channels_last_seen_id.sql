-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Channel pages sort a small heap; the index made every channel touch non-HOT.
DROP INDEX CONCURRENTLY IF EXISTS idx_channels_last_seen_id;
