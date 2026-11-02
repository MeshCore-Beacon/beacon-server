-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Region filters probe iata only; last_heard in the key made every node_iatas update non-HOT.
CREATE INDEX CONCURRENTLY idx_node_iatas_iata_v2 ON node_iatas USING btree (iata);
