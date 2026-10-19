-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Overlay for the local-copy tier of the stats_adverts group and for diff/stats_adverts.yaml, built in the schema
-- t_s7a from public: packet_observations with the columns the adverts statements read and the observer indexes it
-- has between the partial adverts index (migration 034) and the drop of idx_observations_observer_id: the pkey,
-- idx_observations_adverts, idx_observations_observer and idx_observations_observer_id. With both adverts
-- candidates present, a run shows which one the planner prefers.
-- Statements run with search_path "t_s7a, public", so packet_observations resolves here and every other table to
-- public.
--
-- Local copy only: it reads public and writes only inside t_s7a, about 5 GB on a full copy. Remove it with
--   DROP SCHEMA t_s7a CASCADE;
-- Run: psql "$DSN" -v ON_ERROR_STOP=1 -f db/planguard/overlay/stats_adverts.sql
--
-- UNLOGGED, and autovacuum is off so the statistics stay as built. Every row is copied (the full id range), so
-- per-observer numbers compare one to one with public.
\set ON_ERROR_STOP 1
\timing on
SET maintenance_work_mem = '1GB';
SET jit = off;
CREATE SCHEMA IF NOT EXISTS t_s7a;
DROP TABLE IF EXISTS t_s7a.packet_observations;
CREATE UNLOGGED TABLE t_s7a.packet_observations WITH (autovacuum_enabled = false) AS
  SELECT id, heard_at, observer_id, snr, iata, payload_type, rssi, hop_count, packet_hash
  FROM public.packet_observations;
ALTER TABLE t_s7a.packet_observations ADD CONSTRAINT packet_observations_pkey PRIMARY KEY (id);
CREATE INDEX idx_observations_adverts ON t_s7a.packet_observations (observer_id, id) WHERE payload_type = 4;
CREATE INDEX idx_observations_observer ON t_s7a.packet_observations (observer_id, heard_at DESC);
CREATE INDEX idx_observations_observer_id ON t_s7a.packet_observations (observer_id, id);
VACUUM ANALYZE t_s7a.packet_observations;
SELECT c.relname, pg_size_pretty(pg_relation_size(c.oid)) FROM pg_class c
WHERE c.relnamespace = 't_s7a'::regnamespace ORDER BY 1;
