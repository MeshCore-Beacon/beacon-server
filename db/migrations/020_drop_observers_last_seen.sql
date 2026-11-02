-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Observer pages sort a small heap; the index made every presence flush non-HOT.
DROP INDEX CONCURRENTLY IF EXISTS idx_observers_last_seen;
