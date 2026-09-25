-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Endpoints now resolve at read time like path hops. The per-observation
-- snapshots were ~60% of packet_observations on prod.
ALTER TABLE packet_observations DROP COLUMN IF EXISTS resolved_endpoints;
