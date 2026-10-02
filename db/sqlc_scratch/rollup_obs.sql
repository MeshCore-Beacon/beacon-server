-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- sqlc type-check stub only; db/rollup.go creates this as a TEMP table per rolled hour.
CREATE TABLE rollup_obs (
    packet_hash bytea NOT NULL, observer_id uuid NOT NULL, iata character(3) NOT NULL,
    heard_at timestamptz NOT NULL, path_length_byte smallint NOT NULL, hash_size smallint NOT NULL,
    hop_count smallint NOT NULL, path_len integer NOT NULL, rssi smallint, snr real, airtime_ms real,
    payload_type smallint, route_type smallint NOT NULL, origin_pubkey bytea, scope_id integer);
