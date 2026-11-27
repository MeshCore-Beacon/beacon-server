-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Repeater polling: each observer's latest claim (a full snapshot), the targets it lists, the elected order per
-- target, and the readings observers publish. Public keys are bytea so a target can be named before it has a node row.
CREATE TABLE polling_claims (
    observer_public_key bytea PRIMARY KEY CHECK (octet_length(observer_public_key) = 32),
    observer_id uuid NOT NULL REFERENCES observers(id) ON DELETE CASCADE,
    origin text NOT NULL DEFAULT '',
    client text NOT NULL DEFAULT '',
    policy_version text NOT NULL DEFAULT '',
    community boolean NOT NULL DEFAULT false,
    accepts_requests boolean NOT NULL DEFAULT false,
    request_slots smallint NOT NULL DEFAULT 0,
    claimed_at timestamp with time zone NOT NULL,
    received_at timestamp with time zone NOT NULL
);
CREATE INDEX idx_polling_claims_observer ON polling_claims USING btree (observer_id);

CREATE TABLE polling_candidates (
    observer_public_key bytea NOT NULL REFERENCES polling_claims(observer_public_key) ON DELETE CASCADE,
    target_public_key bytea NOT NULL,
    key_mode text NOT NULL DEFAULT '',
    claimed_snr real,
    requested boolean NOT NULL DEFAULT false,
    state text NOT NULL DEFAULT '',
    misses smallint NOT NULL DEFAULT 0,
    last_attempt timestamp with time zone,
    last_ok timestamp with time zone,
    PRIMARY KEY (observer_public_key, target_public_key)
);
CREATE INDEX idx_polling_candidates_target ON polling_candidates USING btree (target_public_key);

CREATE TABLE polling_assignments (
    target_public_key bytea NOT NULL,
    observer_public_key bytea NOT NULL,
    rank smallint NOT NULL,
    evidence_snr real,
    evidence_count integer NOT NULL DEFAULT 0,
    assigned_at timestamp with time zone NOT NULL,
    PRIMARY KEY (target_public_key, observer_public_key)
);
-- At most one poller per target.
CREATE UNIQUE INDEX idx_polling_assignments_poller ON polling_assignments USING btree (target_public_key) WHERE rank = 0;

CREATE TABLE repeater_readings (
    id bigserial PRIMARY KEY,
    target_public_key bytea NOT NULL,
    -- The key is the observer's identity; the row id goes NULL if the observer row is deleted, keeping the history.
    observer_public_key bytea NOT NULL CHECK (octet_length(observer_public_key) = 32),
    observer_id uuid REFERENCES observers(id) ON DELETE SET NULL,
    sampled_at timestamp with time zone NOT NULL,
    received_at timestamp with time zone NOT NULL,
    method text NOT NULL CHECK (method IN ('status', 'telemetry')),
    tag bigint NOT NULL,
    battery_mv integer,
    temperature_c real,
    poll_snr real,
    uptime_secs bigint,
    packets_recv bigint,
    packets_sent bigint,
    recv_flood bigint,
    recv_direct bigint,
    sent_flood bigint,
    sent_direct bigint,
    tx_air_secs bigint,
    rx_air_secs bigint,
    recv_errors bigint,
    noise_floor integer,
    last_rssi integer,
    tx_queue integer,
    flood_dups integer,
    direct_dups integer,
    err_events integer,
    last_snr real,
    lpp bytea CHECK (lpp IS NULL OR octet_length(lpp) <= 160),
    packet_hash bytea CHECK (packet_hash IS NULL OR octet_length(packet_hash) = 8),
    corroborated smallint,
    client text NOT NULL DEFAULT '' CHECK (octet_length(client) <= 128),
    UNIQUE (target_public_key, observer_public_key, tag, method)
);
CREATE INDEX idx_repeater_readings_target ON repeater_readings USING btree (target_public_key, sampled_at DESC);
CREATE INDEX idx_repeater_readings_received_brin ON repeater_readings USING brin (received_at);
CREATE INDEX idx_repeater_readings_observer ON repeater_readings USING btree (observer_id) WHERE observer_id IS NOT NULL;
-- The per-observer rate and budget checks look up recent readings by observer key and receive time.
CREATE INDEX idx_repeater_readings_observer_received ON repeater_readings USING btree (observer_public_key, received_at);

CREATE TABLE polling_observers (
    observer_public_key bytea PRIMARY KEY,
    blocked boolean NOT NULL DEFAULT false,
    approved boolean NOT NULL DEFAULT false,
    note text NOT NULL DEFAULT ''
);

CREATE TABLE polling_state (
    id boolean PRIMARY KEY DEFAULT true CHECK (id),
    paused boolean NOT NULL DEFAULT false,
    paused_until timestamp with time zone,
    excluded_nodes bytea[] NOT NULL DEFAULT '{}'
);
INSERT INTO polling_state (id) VALUES (true);
