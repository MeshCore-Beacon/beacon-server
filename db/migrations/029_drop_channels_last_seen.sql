-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Migration 028 replaces this index with (last_seen DESC, id DESC).
-- Keep this separate for concurrent execution; IF EXISTS allows a completed drop to retry.
DROP INDEX CONCURRENTLY IF EXISTS idx_channels_last_seen;
