-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- No statement filters nodes by coordinates.
DROP INDEX CONCURRENTLY IF EXISTS idx_nodes_location;
