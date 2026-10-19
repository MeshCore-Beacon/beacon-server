-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- No statement reads (last_seen DESC); retention walks idx_known_routes_keyset.
DROP INDEX CONCURRENTLY IF EXISTS idx_known_routes_last_seen;
