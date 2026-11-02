-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Nothing looks up channel sites by IATA; the primary key serves the channel.
DROP INDEX CONCURRENTLY IF EXISTS idx_channel_iatas_iata;
