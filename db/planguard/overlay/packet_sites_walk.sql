-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Overlay for diff/planted_readmodels.yaml: the packet_sites table in its merged form, built in the schema t_s2b
-- from public.packet_observations and public.packets for the 12 sites the walk requests, and nothing else. Any
-- statement whose site list is a subset of these 12 sees the rows a full packet_sites would give it.
--
-- It reads public and writes only inside t_s2b (about 394K rows and 75 MB on a full copy). Remove it with
--   DROP SCHEMA t_s2b CASCADE;
-- Run: psql "$DSN" -v ON_ERROR_STOP=1 -f db/planguard/overlay/packet_sites_walk.sql
--
-- UNLOGGED and without foreign keys (an overlay only; the planner treats it like a logged table). Autovacuum is off
-- so the statistics stay as built. Rows go in ingest order (first observation id); site_heard_at is the packet's
-- newest observation at that site.
SET maintenance_work_mem = '512MB';
SET work_mem = '256MB';
SET jit = off;
CREATE SCHEMA IF NOT EXISTS t_s2b;
DROP TABLE IF EXISTS t_s2b.packet_sites;
CREATE UNLOGGED TABLE t_s2b.packet_sites (
    packet_hash    bytea        NOT NULL,
    iata           character(3) NOT NULL,
    site_heard_at  timestamptz  NOT NULL,
    first_heard_at timestamptz  NOT NULL,
    payload_type   smallint     NOT NULL CHECK (payload_type BETWEEN 0 AND 15),
    route_type     smallint     NOT NULL CHECK (route_type BETWEEN 0 AND 3),
    scope_id       integer
) WITH (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_cost_limit = 1000, autovacuum_enabled = false);
ALTER TABLE t_s2b.packet_sites ALTER COLUMN iata SET STATISTICS 500;
INSERT INTO t_s2b.packet_sites
SELECT po.packet_hash, po.iata, max(po.heard_at), p.first_heard_at, p.payload_type, p.route_type, p.scope_id
FROM public.packet_observations po JOIN public.packets p ON p.packet_hash = po.packet_hash
WHERE po.iata = ANY('{AMS,EIN,ENS,GLZ,GRQ,LEY,LWR,MST,QAR,RTM,UDE,UTC}'::bpchar[])
GROUP BY po.packet_hash, po.iata, p.first_heard_at, p.payload_type, p.route_type, p.scope_id
ORDER BY min(po.id);
ALTER TABLE t_s2b.packet_sites ADD CONSTRAINT packet_sites_pkey PRIMARY KEY (packet_hash, iata) INCLUDE (site_heard_at);
CREATE INDEX idx_packet_sites_site ON t_s2b.packet_sites (iata, site_heard_at DESC, packet_hash DESC)
    INCLUDE (payload_type, route_type, first_heard_at);
CREATE INDEX idx_packet_sites_site_type ON t_s2b.packet_sites (iata, payload_type, route_type, site_heard_at DESC, packet_hash DESC);
CREATE INDEX idx_packet_sites_site_scope ON t_s2b.packet_sites (iata, scope_id, site_heard_at DESC, packet_hash DESC)
    INCLUDE (payload_type, route_type, first_heard_at)
    WHERE scope_id IS NOT NULL;
VACUUM ANALYZE t_s2b.packet_sites;
SELECT count(*) AS rows, count(DISTINCT packet_hash) AS packets, count(DISTINCT iata) AS sites,
       pg_size_pretty(pg_total_relation_size('t_s2b.packet_sites')) AS size
FROM t_s2b.packet_sites;
