-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Trace pages walk the keyset index; a first-heard filter does not need its own index.
DROP INDEX CONCURRENTLY IF EXISTS idx_trace_tags_first_heard;
