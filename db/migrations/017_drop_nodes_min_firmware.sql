-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- No statement filters on min_firmware_version.
DROP INDEX CONCURRENTLY IF EXISTS idx_nodes_min_firmware;
