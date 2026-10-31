-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Never scanned; the type filter reads a small heap.
DROP INDEX CONCURRENTLY IF EXISTS idx_observers_type;
