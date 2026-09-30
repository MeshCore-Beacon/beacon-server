-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Preserve historical identities while distinguishing import-only matching keys.
ALTER TABLE transport_scopes ADD COLUMN IF NOT EXISTS imported_only BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS meshmapper_scope_catalogues (
    iata TEXT NOT NULL,
    url TEXT NOT NULL,
    payload JSONB,
    etag TEXT,
    checked_at TIMESTAMPTZ,
    attempted_at TIMESTAMPTZ NOT NULL,
    next_attempt TIMESTAMPTZ NOT NULL,
    last_error TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (iata, url)
);
