-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- One row per (hour, originating node): how often its packets were heard, and the signal and hop sums behind the
-- averages. The node activity chart reads this instead of every observation.
CREATE TABLE analytics_hourly_node_activity (
    hour timestamp with time zone NOT NULL,
    origin_pubkey bytea NOT NULL,
    observations bigint NOT NULL,
    snr_sum double precision,
    snr_n bigint NOT NULL,
    rssi_sum bigint,
    rssi_n bigint NOT NULL,
    hop_sum bigint NOT NULL,
    hop_n bigint NOT NULL,
    PRIMARY KEY (hour, origin_pubkey)
);

-- The table is new and empty, so the build takes no time and needs no CONCURRENTLY.
CREATE INDEX idx_analytics_node_activity_node ON analytics_hourly_node_activity USING btree (origin_pubkey, hour);
