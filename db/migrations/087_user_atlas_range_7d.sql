-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- A saved Atlas range may be 7d as well as 24h and 3d.
ALTER TABLE user_atlas DROP CONSTRAINT IF EXISTS user_atlas_range_check;
ALTER TABLE user_atlas ADD CONSTRAINT user_atlas_range_check
    CHECK (range IN ('24h', '3d', '7d'));
