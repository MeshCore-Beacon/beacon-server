-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- No statement reads (iata, hop_count); hop filters use idx_known_routes_hop_iata.
DROP INDEX CONCURRENTLY IF EXISTS idx_known_routes_hop_count;
