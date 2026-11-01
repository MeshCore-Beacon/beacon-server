-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Superseded by end_state.sql for the local tier; kept as the evidence of its change and for its diff cases.
-- Overlay for the local-copy tier of the packet_sites group, diff/packet_sites.yaml and storediff/packet_sites.yaml:
-- it builds t_s6a; it never writes public. It holds
--   t_s6a.packet_sites      the migrations' table and indexes, filled from public as the backfill fills it (one row
--                           per packet and site at its newest hearing there), plus the stale site NEW: a copy of the
--                           rows of the tenth busiest recent site, inserted after ANALYZE so the statistics never
--                           see it;
--   t_s6a.background_state  only the packet_sites state, complete and verified, so a read-only server trusts the
--                           copy (for ten minutes after this script, or after its updated_at is refreshed).
-- Statements run with search_path "t_s6a, public", so these two resolve here and every other table to public.
-- UNLOGGED and without foreign keys (an overlay only; the planner treats it like a logged table); autovacuum off
-- so the statistics stay as built. ANALYZE runs once, before the stale site; VACUUM runs twice after it, since a
-- snapshot still open during the first leaves the visibility map unset. About 1.6 GB on a full copy.
--
-- The group's resolvers read packet_sites too, and resolvers run on the run's base search_path, so run its local
-- tier with that path:
--   planguard run --tier local --only '^ps[.]' --search-path 't_s6a, public' --dsn ...
--
-- Local copy only. Remove it with
--   DROP SCHEMA t_s6a CASCADE;
-- Run: psql "$DSN" -v ON_ERROR_STOP=1 -f db/planguard/overlay/packet_sites.sql
\set ON_ERROR_STOP 1
\timing on
SET maintenance_work_mem = '1GB';
SET work_mem = '256MB';
SET jit = off;
CREATE SCHEMA IF NOT EXISTS t_s6a;
DROP TABLE IF EXISTS t_s6a.packet_sites, t_s6a.background_state;

CREATE UNLOGGED TABLE t_s6a.packet_sites (
    packet_hash    bytea        NOT NULL,
    iata           character(3) NOT NULL,
    site_heard_at  timestamptz  NOT NULL,
    first_heard_at timestamptz  NOT NULL,
    payload_type   smallint     NOT NULL CHECK (payload_type BETWEEN 0 AND 15),
    route_type     smallint     NOT NULL CHECK (route_type BETWEEN 0 AND 3),
    scope_id       integer
) WITH (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_cost_limit = 1000, autovacuum_enabled = false);
ALTER TABLE t_s6a.packet_sites ALTER COLUMN iata SET STATISTICS 500;
INSERT INTO t_s6a.packet_sites
SELECT po.packet_hash, po.iata, max(po.heard_at), p.first_heard_at, p.payload_type, p.route_type, p.scope_id
FROM public.packet_observations po JOIN public.packets p ON p.packet_hash = po.packet_hash
GROUP BY po.packet_hash, po.iata, p.first_heard_at, p.payload_type, p.route_type, p.scope_id
ORDER BY min(po.id);
ALTER TABLE t_s6a.packet_sites ADD CONSTRAINT packet_sites_pkey PRIMARY KEY (packet_hash, iata) INCLUDE (site_heard_at);
CREATE INDEX idx_packet_sites_site ON t_s6a.packet_sites USING btree (iata, site_heard_at DESC, packet_hash DESC) INCLUDE (payload_type, route_type, first_heard_at);
CREATE INDEX idx_packet_sites_site_type ON t_s6a.packet_sites USING btree (iata, payload_type, route_type, site_heard_at DESC, packet_hash DESC);
CREATE INDEX idx_packet_sites_site_scope ON t_s6a.packet_sites USING btree (iata, scope_id, site_heard_at DESC, packet_hash DESC) INCLUDE (payload_type, route_type, first_heard_at) WHERE (scope_id IS NOT NULL);
VACUUM ANALYZE t_s6a.packet_sites;

-- The stale site, after the ANALYZE.
INSERT INTO t_s6a.packet_sites
SELECT ps.packet_hash, 'NEW', ps.site_heard_at, ps.first_heard_at, ps.payload_type, ps.route_type, ps.scope_id
FROM t_s6a.packet_sites ps
WHERE ps.iata = (SELECT po.iata FROM public.packet_observations po
                 WHERE po.id > (SELECT max(id) - 200000 FROM public.packet_observations)
                 GROUP BY po.iata ORDER BY count(*) DESC, po.iata OFFSET 9 LIMIT 1)
ORDER BY ps.site_heard_at;

CREATE TABLE t_s6a.background_state (key text PRIMARY KEY, value jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
INSERT INTO t_s6a.background_state (key, value)
SELECT 'packet_sites', jsonb_build_object('writer_from_id', o.hi, 'backfilled_down_to_id', 0, 'complete', true,
         'recon_prev_hi', o.hi, 'recon_hi', o.hi,
         'first_verified_at', to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'))
FROM (SELECT coalesce(max(id), 0) hi FROM public.packet_observations) o;

VACUUM t_s6a.packet_sites;
VACUUM t_s6a.packet_sites;
DO $$
BEGIN
  IF (SELECT relallvisible < relpages FROM pg_class WHERE oid = 't_s6a.packet_sites'::regclass) THEN
    RAISE EXCEPTION 't_s6a.packet_sites: the visibility map is not set on every page';
  END IF;
END $$;
SELECT c.relname, pg_size_pretty(pg_relation_size(c.oid)) size, c.reltuples::bigint
FROM pg_class c WHERE c.relnamespace = 't_s6a'::regnamespace ORDER BY 1;
