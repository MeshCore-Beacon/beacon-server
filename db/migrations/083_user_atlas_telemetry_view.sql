-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- A node pin may open on the Telemetry view and keep its own four tiles for it, apart from the Observer view's tiles.
ALTER TABLE user_atlas_pins ADD COLUMN telemetry_tiles text[];
ALTER TABLE user_atlas_pins DROP CONSTRAINT IF EXISTS user_atlas_pins_view_check;
ALTER TABLE user_atlas_pins ADD CONSTRAINT user_atlas_pins_view_check
    CHECK (view IS NULL OR view IN ('heard', 'observer', 'telemetry'));
