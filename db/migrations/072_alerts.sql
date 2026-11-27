-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Alert rules on Atlas radios, the push tokens of app sessions, and the outbox the delivery task drains. Every
-- table that belongs to a user goes with the user.
CREATE TABLE user_alert_rules (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    target_kind text NOT NULL CHECK (target_kind IN ('node', 'observer')),
    target_public_key bytea NOT NULL CHECK (octet_length(target_public_key) = 32),
    kind text NOT NULL CHECK (kind IN ('gone_quiet', 'back_online', 'battery_low', 'battery_falling', 'rebooted',
                                       'temperature', 'noise_high', 'clock', 'observer_offline')),
    threshold integer,
    threshold_high integer,
    enabled boolean NOT NULL DEFAULT true,
    state text NOT NULL DEFAULT 'unknown' CHECK (state IN ('ok', 'firing', 'inactive', 'unknown')),
    state_changed_at timestamp with time zone NOT NULL DEFAULT now(),
    last_sent_at timestamp with time zone,
    -- Newest data the evaluator has already looked at (reboots, adverts), so an event is judged once.
    cursor_at timestamp with time zone NOT NULL DEFAULT now(),
    streak smallint NOT NULL DEFAULT 0,
    -- When a gone_quiet rule last went from firing to ok; the back_online rule of the same radio reads it.
    recovered_at timestamp with time zone,
    -- Bumped by every edit, so an evaluation that started before the edit cannot overwrite it.
    version bigint NOT NULL DEFAULT 0,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    UNIQUE (user_id, target_public_key, kind)
);
CREATE INDEX idx_user_alert_rules_enabled ON user_alert_rules USING btree (id) WHERE enabled;

CREATE TABLE user_devices (
    session_id uuid PRIMARY KEY REFERENCES user_sessions(id) ON DELETE CASCADE,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    platform text NOT NULL CHECK (platform IN ('ios', 'android')),
    push_token text NOT NULL,
    updated_at timestamp with time zone NOT NULL DEFAULT now()
);
CREATE INDEX idx_user_devices_user ON user_devices USING btree (user_id);
CREATE INDEX idx_user_devices_token ON user_devices USING btree (push_token);

CREATE TABLE alert_outbox (
    id bigserial PRIMARY KEY,
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- Everything one evaluation pass produced for a user shares a pass id, so email goes out as one message.
    pass_id uuid NOT NULL,
    target_kind text NOT NULL,
    target_public_key bytea NOT NULL,
    name text NOT NULL DEFAULT '',
    alert_kind text NOT NULL,
    value bigint,
    detail text NOT NULL DEFAULT '',
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    attempts smallint NOT NULL DEFAULT 0,
    delivered_at timestamp with time zone
);
CREATE INDEX idx_alert_outbox_pending ON alert_outbox USING btree (created_at) WHERE delivered_at IS NULL;
CREATE INDEX idx_alert_outbox_delivered ON alert_outbox USING btree (delivered_at) WHERE delivered_at IS NOT NULL;

-- The status a request was last announced at, so a request needing attention is announced once.
ALTER TABLE telemetry_requests ADD COLUMN attention_status text;
