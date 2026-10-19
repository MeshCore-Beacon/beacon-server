-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- The tables testdata/<name>.csv loads into (schema planguard_src). The CSVs hold aggregate distributions of a
-- production-sized copy only: counts by region, hour, type, rank and percentile. They hold no rows, keys, names
-- or identifiers; observers, scopes and channels appear as ranks.
SET search_path = planguard_src;

-- known_routes
CREATE TABLE routes_regions (iata char(3) NOT NULL, prod_rows bigint NOT NULL);           -- routes per region
CREATE TABLE routes_iata_hop (iata char(3) NOT NULL, hop_count int NOT NULL, n bigint NOT NULL);
CREATE TABLE routes_age_obs (age_h int NOT NULL, obs_ge3 boolean NOT NULL, n bigint NOT NULL);
CREATE TABLE routes_obs (obs bigint NOT NULL, ge3 boolean NOT NULL, n bigint NOT NULL);
CREATE TABLE routes_relay (rank int NOT NULL, n bigint NOT NULL);                         -- relay popularity, densest region
-- packets
CREATE TABLE packets_hour (age_h int NOT NULL, n bigint NOT NULL);
CREATE TABLE packets_types (payload_type smallint NOT NULL, route_type smallint NOT NULL, n bigint NOT NULL,
  n_channel bigint NOT NULL, n_origin bigint NOT NULL, n_trace bigint NOT NULL, n_scoped bigint NOT NULL);
CREATE TABLE packets_scope (rank int NOT NULL, n bigint NOT NULL);
CREATE TABLE packets_obscount (obs int NOT NULL, n bigint NOT NULL);
CREATE TABLE packets_life (p int NOT NULL, life_s float8 NOT NULL);
-- packet_observations
CREATE TABLE po_iata (iata char(3) NOT NULL, n bigint NOT NULL);
CREATE TABLE po_sites_per_packet (n_sites int NOT NULL, packets bigint NOT NULL);
CREATE TABLE po_observer (rank int NOT NULL, n bigint NOT NULL, n_adverts bigint NOT NULL, home_iata char(3) NOT NULL,
  home_n bigint NOT NULL);
CREATE TABLE po_delay (p int NOT NULL, delay_s float8 NOT NULL);
CREATE TABLE po_hop (hop_count smallint NOT NULL, n bigint NOT NULL);
CREATE TABLE po_whole_second (whole_second_pct numeric NOT NULL);
-- channel messages by channel rank, with the site that hears each channel most
CREATE TABLE cm_channel (rank int NOT NULL, n bigint NOT NULL, home_iata char(3), home_n bigint);
-- nodes
CREATE TABLE nodes_types (node_type smallint NOT NULL, n bigint NOT NULL, n_drift60 bigint NOT NULL, n_drift bigint NOT NULL);
CREATE TABLE node_iatas_iata (iata char(3) NOT NULL, n bigint NOT NULL);

RESET search_path;
