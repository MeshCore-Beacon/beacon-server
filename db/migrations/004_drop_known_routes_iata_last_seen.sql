-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- No statement reads (iata, last_seen DESC); route pages use idx_known_routes_iata_keyset.
DROP INDEX CONCURRENTLY IF EXISTS idx_known_routes_iata_last_seen;
