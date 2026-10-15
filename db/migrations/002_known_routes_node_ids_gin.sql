-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Cross-IATA route search looks routes up by member node.
CREATE INDEX CONCURRENTLY idx_known_routes_node_ids ON known_routes USING gin (node_ids);
