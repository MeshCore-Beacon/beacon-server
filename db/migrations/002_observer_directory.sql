-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

CREATE TABLE observer_directory_snapshots (
    id uuid PRIMARY KEY,
    query_key bytea NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL DEFAULT now() + interval '15 minutes',
    metadata jsonb NOT NULL,
    items jsonb NOT NULL,
    CHECK (jsonb_typeof(items) = 'array'),
    CHECK (pg_column_size(items) <= 16777216)
);
CREATE INDEX observer_directory_snapshots_expiry ON observer_directory_snapshots (expires_at);
