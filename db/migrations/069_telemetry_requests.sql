-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Telemetry requests: a signed-in user asks Beacon to have a repeater polled, optionally with its guest password.
-- A password is sealed (AES-256-GCM) before it reaches this table and is never returned by any route.
CREATE TABLE telemetry_requests (
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    target_public_key bytea NOT NULL CHECK (octet_length(target_public_key) = 32),
    status text NOT NULL CHECK (status IN ('waiting', 'active', 'login_failed', 'admin_stopped', 'disabled')),
    status_reason text NOT NULL DEFAULT '',
    password_ct bytea,
    password_nonce bytea,
    password_failed boolean NOT NULL DEFAULT false,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    -- Created or last re-enabled: the offline rule counts from here, so a re-enabled request is not disabled again at once.
    enabled_at timestamp with time zone NOT NULL,
    CHECK ((password_ct IS NULL) = (password_nonce IS NULL)),
    UNIQUE (user_id, target_public_key)
);
CREATE INDEX idx_telemetry_requests_target ON telemetry_requests USING btree (target_public_key, created_at);

-- One row per repeater ever requested. ver only goes up; no_password_blocked stays until an operator clears it.
CREATE TABLE polling_targets (
    target_public_key bytea PRIMARY KEY CHECK (octet_length(target_public_key) = 32),
    active_request_id uuid REFERENCES telemetry_requests(id) ON DELETE SET NULL,
    ver bigint NOT NULL DEFAULT 0,
    ver_changed_at timestamp with time zone,
    no_password_blocked boolean NOT NULL DEFAULT false
);

-- An observer whose device was given admin access by a requested repeater is never offered that repeater again.
CREATE TABLE polling_admin_stops (
    target_public_key bytea NOT NULL,
    observer_public_key bytea NOT NULL,
    reported_at timestamp with time zone NOT NULL,
    PRIMARY KEY (target_public_key, observer_public_key)
);

-- When an observer's claim last showed a state other than login_failed for a requested repeater, for the rule that
-- ignores login_failed right after a password change.
CREATE TABLE polling_other_states (
    target_public_key bytea NOT NULL,
    observer_public_key bytea NOT NULL,
    seen_at timestamp with time zone NOT NULL,
    PRIMARY KEY (target_public_key, observer_public_key)
);

-- True for an assignment made because the repeater is requested; the observer is then handed its password.
ALTER TABLE polling_assignments ADD COLUMN requested boolean NOT NULL DEFAULT false;
