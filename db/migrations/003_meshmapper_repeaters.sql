-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- One source per MeshMapper group, or per IATA outside a group: any member's site lists the whole group.
CREATE TABLE IF NOT EXISTS meshmapper_repeater_sources (
    source text PRIMARY KEY,
    url text NOT NULL,
    etag text,
    checked_at timestamp with time zone,
    attempted_at timestamp with time zone NOT NULL,
    next_attempt timestamp with time zone NOT NULL,
    last_error text DEFAULT ''::text NOT NULL
);

CREATE TABLE IF NOT EXISTS meshmapper_repeaters (
    source text NOT NULL,
    public_key bytea NOT NULL,
    data jsonb NOT NULL,
    PRIMARY KEY (source, public_key)
);

CREATE INDEX IF NOT EXISTS idx_meshmapper_repeaters_public_key ON meshmapper_repeaters (public_key);
