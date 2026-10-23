-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- No statement reads (last_reconfirmed_at); reconfirm no longer scans by it.
DROP INDEX CONCURRENTLY IF EXISTS idx_known_routes_reconfirm;
