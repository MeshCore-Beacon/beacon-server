-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Default 20% trigger never fired on known_routes, bloating its indexes.
ALTER TABLE known_routes SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_analyze_scale_factor = 0.02,
  autovacuum_vacuum_cost_limit = 1000
);
ALTER TABLE packets SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_cost_limit = 1000);
ALTER TABLE packet_observations SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_cost_limit = 1000);
ALTER TABLE nodes SET (autovacuum_vacuum_scale_factor = 0.05);
ALTER TABLE node_iatas SET (autovacuum_vacuum_scale_factor = 0.05);
ALTER TABLE node_short_ids SET (autovacuum_vacuum_scale_factor = 0.05);
