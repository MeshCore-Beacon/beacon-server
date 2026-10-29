-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Regions are read by id through the primary key; the IATA lookup reads a table of a few hundred rows.
DROP INDEX CONCURRENTLY IF EXISTS idx_region_iatas_iata;
