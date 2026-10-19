-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Duplicates nodes_public_key_key.
DROP INDEX CONCURRENTLY IF EXISTS idx_nodes_pubkey;
