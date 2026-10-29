-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Channel pages read the members by IATA through the primary key.
DROP INDEX CONCURRENTLY IF EXISTS idx_meshmapper_channel_members_fingerprint;
