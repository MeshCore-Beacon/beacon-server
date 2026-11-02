-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- One row per (packet, site that heard it) at the site's latest hearing; region packet pages read this
-- instead of every observation. Rows go with their packet.
CREATE TABLE packet_sites (
    packet_hash    bytea        NOT NULL REFERENCES packets(packet_hash) ON DELETE CASCADE,
    iata           character(3) NOT NULL,
    site_heard_at  timestamptz  NOT NULL,
    first_heard_at timestamptz  NOT NULL,
    payload_type   smallint     NOT NULL CHECK (payload_type BETWEEN 0 AND 15),
    route_type     smallint     NOT NULL CHECK (route_type BETWEEN 0 AND 3),
    scope_id       integer,
    PRIMARY KEY (packet_hash, iata) INCLUDE (site_heard_at)
) WITH (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_cost_limit = 1000);
ALTER TABLE packet_sites ALTER COLUMN iata SET STATISTICS 500;
