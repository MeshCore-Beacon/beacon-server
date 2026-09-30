-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Imported MeshMapper region boundaries. Kept apart from iata_codes.border so
-- startup seeding of manual borderFile borders never undoes the override.
CREATE TABLE IF NOT EXISTS meshmapper_zone_boundaries (
    iata TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    feature JSONB,
    etag TEXT,
    checked_at TIMESTAMPTZ,
    attempted_at TIMESTAMPTZ NOT NULL,
    next_attempt TIMESTAMPTZ NOT NULL,
    last_error TEXT NOT NULL DEFAULT ''
);
