-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- The primary key (node_id, neighbor_id, iata) serves every lookup by node.
DROP INDEX CONCURRENTLY IF EXISTS idx_node_neighbors_node;
