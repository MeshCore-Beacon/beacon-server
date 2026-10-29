-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Custom typeName pages seq-scan instead; every advert update was non-HOT for it.
DROP INDEX CONCURRENTLY IF EXISTS idx_nodes_type_last_seen;
