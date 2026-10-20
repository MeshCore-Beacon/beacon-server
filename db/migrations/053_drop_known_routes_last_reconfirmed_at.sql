-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Reconfirm no longer stamps routes; no statement reads the column.
ALTER TABLE known_routes DROP COLUMN last_reconfirmed_at;
