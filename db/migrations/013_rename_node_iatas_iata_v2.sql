-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Keep the index name the (iata) foreign key cascade and the plans have always used.
ALTER INDEX idx_node_iatas_iata_v2 RENAME TO idx_node_iatas_iata;
