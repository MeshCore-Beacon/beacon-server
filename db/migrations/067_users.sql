-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- User accounts, their login sessions and one-time tokens. Only SHA-256 hashes of session and one-time tokens are
-- stored. Not related to the operator `accounts` table.
CREATE TABLE users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    username text NOT NULL,
    display_name text NOT NULL,
    email text NOT NULL,
    pending_email text,
    pass_hash text NOT NULL,
    email_verified_at timestamp with time zone,
    provider text NOT NULL DEFAULT 'local',
    provider_subject text,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    last_login_at timestamp with time zone,
    disabled_at timestamp with time zone
);
CREATE UNIQUE INDEX idx_users_username ON users USING btree (lower(username));
CREATE UNIQUE INDEX idx_users_email ON users USING btree (email);
CREATE UNIQUE INDEX idx_users_provider_subject ON users USING btree (provider, provider_subject) WHERE provider_subject IS NOT NULL;
-- ListUsers pages newest first.
CREATE INDEX idx_users_created ON users USING btree (created_at DESC, id DESC);

CREATE TABLE user_sessions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash bytea NOT NULL UNIQUE,
    kind text NOT NULL CHECK (kind IN ('web', 'app')),
    label text NOT NULL DEFAULT '',
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    last_used_at timestamp with time zone NOT NULL DEFAULT now(),
    expires_at timestamp with time zone NOT NULL,
    hard_expires_at timestamp with time zone,
    ended_at timestamp with time zone,
    ip text NOT NULL DEFAULT '',
    user_agent text NOT NULL DEFAULT ''
);
CREATE INDEX idx_user_sessions_user ON user_sessions USING btree (user_id);

CREATE TABLE user_tokens (
    token_hash bytea PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    kind text NOT NULL,
    data jsonb,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    expires_at timestamp with time zone NOT NULL,
    used_at timestamp with time zone
);
CREATE INDEX idx_user_tokens_user_kind ON user_tokens USING btree (user_id, kind);
