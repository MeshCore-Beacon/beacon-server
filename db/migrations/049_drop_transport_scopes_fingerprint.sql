-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Scopes are loaded whole; nothing looks one up by key fingerprint.
DROP INDEX CONCURRENTLY IF EXISTS idx_transport_scopes_fingerprint;
