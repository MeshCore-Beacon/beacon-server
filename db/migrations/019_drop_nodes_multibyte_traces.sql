-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- The multibyte filters cost a few dozen extra buffers on a seq scan.
DROP INDEX CONCURRENTLY IF EXISTS idx_nodes_multibyte_traces;
