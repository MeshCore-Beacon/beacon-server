-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- A signed-in user's Atlas: the range and up to twelve ordered pins. Pins are stored by public key so a node or
-- observer row can come and go; ids are resolved on read. The revision guards whole-collection replacement.
CREATE TABLE user_atlas (
    user_id uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    range text NOT NULL DEFAULT '24h' CHECK (range IN ('24h', '3d')),
    revision bigint NOT NULL DEFAULT 0,
    updated_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE TABLE user_atlas_pins (
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    position smallint NOT NULL CHECK (position >= 0),
    kind text NOT NULL CHECK (kind IN ('node', 'observer')),
    public_key bytea NOT NULL CHECK (octet_length(public_key) = 32),
    name text NOT NULL DEFAULT '',
    tiles text[],
    view text CHECK (view IS NULL OR view IN ('heard', 'observer')),
    PRIMARY KEY (user_id, position),
    UNIQUE (user_id, kind, public_key)
);
