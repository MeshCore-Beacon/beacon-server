-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Superseded by end_state.sql for the local tier; kept as the evidence of its change and for its diff cases.
-- Overlay for the local-copy runs of the lookup-table entries, built in the schema t_s10 from public: the six lookup
-- tables whose cold index the migrations drop (channel_iatas, node_neighbors, transport_scopes,
-- meshmapper_channel_members, region_iatas, trace_tags), each with the indexes it keeps and none of the dropped ones.
-- Statements run with search_path "t_s10, public", so those six resolve here and every other table to public.
--
-- Local copy only: it reads public and writes only inside t_s10, about 170 MB on a full copy. Remove it with
--   DROP SCHEMA t_s10 CASCADE;
-- Run: psql "$DSN" -v ON_ERROR_STOP=1 -f db/planguard/overlay/small_tables_end_state.sql
\set ON_ERROR_STOP 1
\timing on
SET maintenance_work_mem = '1GB';
SET jit = off;
CREATE SCHEMA IF NOT EXISTS t_s10;
DROP TABLE IF EXISTS t_s10.channel_iatas, t_s10.node_neighbors, t_s10.transport_scopes,
  t_s10.meshmapper_channel_members, t_s10.region_iatas, t_s10.trace_tags;

CREATE TABLE t_s10.channel_iatas AS SELECT * FROM public.channel_iatas;
ALTER TABLE t_s10.channel_iatas ADD CONSTRAINT channel_iatas_pkey PRIMARY KEY (channel_hash, iata);

CREATE TABLE t_s10.node_neighbors AS SELECT * FROM public.node_neighbors;
ALTER TABLE t_s10.node_neighbors ADD CONSTRAINT node_neighbors_pkey PRIMARY KEY (node_id, neighbor_id, iata);
CREATE INDEX idx_node_neighbors_neighbor ON t_s10.node_neighbors (neighbor_id, iata);

CREATE TABLE t_s10.transport_scopes AS SELECT * FROM public.transport_scopes;
ALTER TABLE t_s10.transport_scopes ADD CONSTRAINT transport_scopes_pkey PRIMARY KEY (id);
ALTER TABLE t_s10.transport_scopes ADD CONSTRAINT transport_scopes_name_key UNIQUE (name);

CREATE TABLE t_s10.meshmapper_channel_members AS SELECT * FROM public.meshmapper_channel_members;
ALTER TABLE t_s10.meshmapper_channel_members ADD CONSTRAINT meshmapper_channel_members_pkey PRIMARY KEY (iata, key_fingerprint);

CREATE TABLE t_s10.region_iatas AS SELECT * FROM public.region_iatas;
ALTER TABLE t_s10.region_iatas ADD CONSTRAINT region_iatas_pkey PRIMARY KEY (region_id, iata);

CREATE TABLE t_s10.trace_tags AS SELECT * FROM public.trace_tags;
ALTER TABLE t_s10.trace_tags ADD CONSTRAINT trace_tags_pkey PRIMARY KEY (trace_tag);
CREATE INDEX idx_trace_tags_last_heard ON t_s10.trace_tags (last_heard_at DESC);
CREATE INDEX idx_trace_tags_keyset ON t_s10.trace_tags (date_trunc('milliseconds', last_heard_at, 'UTC') DESC, trace_tag DESC);

VACUUM ANALYZE t_s10.channel_iatas, t_s10.node_neighbors, t_s10.transport_scopes, t_s10.meshmapper_channel_members,
  t_s10.region_iatas, t_s10.trace_tags;
