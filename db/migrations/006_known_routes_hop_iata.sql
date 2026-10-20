-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- hopCount pages: rare and absent hops read a few entries instead of a whole region or table.
CREATE INDEX CONCURRENTLY idx_known_routes_hop_iata ON known_routes USING btree (hop_count, iata);
