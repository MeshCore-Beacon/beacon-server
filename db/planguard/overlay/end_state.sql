-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Overlay for the local-copy tier of every group: the big tables as a database migrated through every file holds
-- them, built in the schema t_end from public. It holds
--   packets                with its migrated index set and the statistics object;
--   nodes, node_iatas, observers
--                          with the index set and fillfactor of migrations 011-024 and 058, for a copy taken before them;
--   known_routes           with the index set of migrations 004-009 and without the column 053 drops;
--   channel_iatas, node_neighbors, transport_scopes, meshmapper_channel_members, region_iatas, trace_tags
--                          with the indexes they keep after migrations 047-052;
--   packet_observations    every column and every migrated index, including the partial adverts index;
--   channel_messages       with its channel walk index;
--   channel_message_iatas, packet_sites
--                          the read models, filled as their backfills fill them, plus the stale site NEW in
--                          packet_sites (the rows of the tenth busiest recent site, inserted after ANALYZE so the
--                          statistics never see it);
--   background_state       the cmi and packet_sites rows (complete and verified) and observation payload types done.
-- Every index matches a database migrated through every file, so the older indexes the read models did not replace
-- are here and the dropped one (idx_observations_observer_id) is not. To see the plans without an index, drop it
-- inside a transaction on t_end and roll back.
-- Statements run with search_path "t_end, public", so these tables resolve here and every other table to public.
--
-- Local copy only: it reads public and writes only inside t_end, about 19 GB on a full copy. Foreign keys only point
-- at t_end tables (one to a public table would add triggers there). Remove it with
--   DROP SCHEMA t_end CASCADE;
-- Run: psql "$DSN" -v ON_ERROR_STOP=1 -f db/planguard/overlay/end_state.sql
--
-- UNLOGGED, and autovacuum is off so the statistics stay as built. packets.parsed_payload is not copied (NULL): it
-- only feeds the projection's summary text, so plans and buffer counts are the same, but result rows differ in
-- summary; compare API results against public instead. VACUUM ANALYZE runs twice: a snapshot still open during the
-- first leaves the visibility map unset.
\set ON_ERROR_STOP 1
\timing on
SET maintenance_work_mem = '1GB';
SET work_mem = '256MB';
SET jit = off;
DROP SCHEMA IF EXISTS t_end CASCADE;
CREATE SCHEMA t_end;

-- packets
CREATE UNLOGGED TABLE t_end.packets (LIKE public.packets INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
  WITH (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_cost_limit = 1000, autovacuum_enabled = false);
INSERT INTO t_end.packets
SELECT packet_hash, payload_type, payload_version, route_type, transport_codes_present, region_code, sub_region_code, scope_id,
       origin_pubkey, raw_payload, raw_header, NULL::jsonb, decrypted, channel_hash, trace_tag, first_heard_at, last_heard_at, observation_count
FROM public.packets;
ALTER TABLE t_end.packets ADD CONSTRAINT packets_pkey PRIMARY KEY (packet_hash);
ALTER TABLE t_end.packets ADD CONSTRAINT packets_payload_type_domain CHECK (payload_type BETWEEN 0 AND 15);
ALTER TABLE t_end.packets ADD CONSTRAINT packets_route_type_domain CHECK (route_type BETWEEN 0 AND 3);
CREATE INDEX idx_packets_last_heard_hash ON t_end.packets (last_heard_at DESC, packet_hash DESC);
CREATE INDEX idx_packets_type_last_heard ON t_end.packets (payload_type, route_type, last_heard_at DESC, packet_hash DESC);
CREATE INDEX idx_packets_scope_last_heard ON t_end.packets (scope_id, last_heard_at DESC, packet_hash DESC)
  INCLUDE (payload_type, route_type, first_heard_at) WHERE scope_id IS NOT NULL;
CREATE INDEX idx_packets_heard_inverted ON t_end.packets (first_heard_at)
  INCLUDE (last_heard_at, packet_hash, payload_type, route_type, scope_id) WHERE (first_heard_at > last_heard_at) IS TRUE;
CREATE INDEX idx_packets_origin ON t_end.packets (origin_pubkey, first_heard_at DESC) WHERE origin_pubkey IS NOT NULL;
CREATE INDEX idx_packets_channel ON t_end.packets (channel_hash, first_heard_at DESC) WHERE channel_hash IS NOT NULL;
CREATE INDEX idx_packets_trace_tag ON t_end.packets (trace_tag) WHERE trace_tag IS NOT NULL;
CREATE INDEX idx_packets_payload_type ON t_end.packets USING btree (payload_type, first_heard_at DESC);
CREATE INDEX idx_packets_route_type ON t_end.packets USING btree (route_type, first_heard_at DESC);
CREATE INDEX idx_packets_last_heard ON t_end.packets USING btree (last_heard_at DESC);
CREATE INDEX idx_packets_scope ON t_end.packets USING btree (scope_id) WHERE (scope_id IS NOT NULL);
CREATE INDEX idx_packets_first_heard_brin ON t_end.packets USING brin (first_heard_at);
-- Created before VACUUM ANALYZE so the extended statistics are collected.
CREATE STATISTICS t_end.packets_heard_inverted_stat ON ((first_heard_at > last_heard_at)) FROM t_end.packets;

-- nodes, node_iatas and observers: every public index and key except the ones migrations 011-024 drop, node_iatas'
-- (iata) index in place of (iata, last_heard DESC), and nodes and observers at fillfactor 50 (migrations 058 and 024).
DO $$
DECLARE
  t record; ix record; c record;
  drops text[] := ARRAY['idx_nodes_type_last_seen', 'idx_nodes_pubkey', 'idx_nodes_location', 'idx_nodes_min_firmware',
    'idx_nodes_multibyte_paths', 'idx_nodes_multibyte_traces', 'idx_observers_last_seen', 'idx_observers_pubkey',
    'idx_observers_type', 'idx_node_iatas_iata'];
BEGIN
  FOR t IN SELECT * FROM (VALUES ('nodes', 50), ('node_iatas', 100), ('observers', 50)) v(name, ff) LOOP
    EXECUTE format('CREATE UNLOGGED TABLE t_end.%I (LIKE public.%I INCLUDING DEFAULTS INCLUDING CONSTRAINTS) '
                   'WITH (fillfactor = %s, autovacuum_enabled = false)', t.name, t.name, t.ff);
    EXECUTE format('INSERT INTO t_end.%I SELECT * FROM public.%I', t.name, t.name);
    FOR c IN SELECT conname, pg_get_constraintdef(oid) AS def FROM pg_constraint
             WHERE conrelid = format('public.%I', t.name)::regclass AND contype IN ('p', 'u') LOOP
      EXECUTE format('ALTER TABLE t_end.%I ADD CONSTRAINT %I %s', t.name, c.conname, c.def);
    END LOOP;
    FOR ix IN SELECT i.indexname, i.indexdef FROM pg_indexes i
              WHERE i.schemaname = 'public' AND i.tablename = t.name
                AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conname = i.indexname
                                AND k.conrelid = format('public.%I', t.name)::regclass) LOOP
      IF ix.indexname = ANY (drops) THEN CONTINUE; END IF;
      EXECUTE replace(ix.indexdef, ' ON public.', ' ON t_end.');
    END LOOP;
  END LOOP;
END $$;
CREATE INDEX idx_node_iatas_iata ON t_end.node_iatas (iata);

-- known_routes: every public index except the four migrations 004-009 drop, plus their (hop_count, iata) index.
CREATE UNLOGGED TABLE t_end.known_routes (LIKE public.known_routes INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
  WITH (autovacuum_enabled = false);
INSERT INTO t_end.known_routes SELECT * FROM public.known_routes;
ALTER TABLE t_end.known_routes DROP COLUMN IF EXISTS last_reconfirmed_at; -- migration 053
DO $$
DECLARE
  ix record; c record;
  drops text[] := ARRAY['idx_known_routes_iata_last_seen', 'idx_known_routes_last_seen', 'idx_known_routes_hop_count',
    'idx_known_routes_reconfirm'];
BEGIN
  FOR c IN SELECT conname, pg_get_constraintdef(oid) AS def FROM pg_constraint
           WHERE conrelid = 'public.known_routes'::regclass AND contype IN ('p', 'u') LOOP
    EXECUTE format('ALTER TABLE t_end.known_routes ADD CONSTRAINT %I %s', c.conname, c.def);
  END LOOP;
  FOR ix IN SELECT i.indexname, i.indexdef FROM pg_indexes i
            WHERE i.schemaname = 'public' AND i.tablename = 'known_routes'
              AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conname = i.indexname
                              AND k.conrelid = 'public.known_routes'::regclass) LOOP
    IF ix.indexname = ANY (drops) THEN CONTINUE; END IF;
    EXECUTE replace(ix.indexdef, ' ON public.', ' ON t_end.');
  END LOOP;
END $$;
CREATE INDEX IF NOT EXISTS idx_known_routes_hop_iata ON t_end.known_routes USING btree (hop_count, iata);
VACUUM ANALYZE t_end.known_routes;
VACUUM t_end.known_routes;

-- The six lookup tables whose cold index migrations 047-052 drop, each with the indexes it keeps.
CREATE UNLOGGED TABLE t_end.channel_iatas WITH (autovacuum_enabled = false) AS SELECT * FROM public.channel_iatas;
ALTER TABLE t_end.channel_iatas ADD CONSTRAINT channel_iatas_pkey PRIMARY KEY (channel_hash, iata);
CREATE UNLOGGED TABLE t_end.node_neighbors WITH (autovacuum_enabled = false) AS SELECT * FROM public.node_neighbors;
ALTER TABLE t_end.node_neighbors ADD CONSTRAINT node_neighbors_pkey PRIMARY KEY (node_id, neighbor_id, iata);
CREATE INDEX idx_node_neighbors_neighbor ON t_end.node_neighbors (neighbor_id, iata);
CREATE UNLOGGED TABLE t_end.transport_scopes WITH (autovacuum_enabled = false) AS SELECT * FROM public.transport_scopes;
ALTER TABLE t_end.transport_scopes ADD CONSTRAINT transport_scopes_pkey PRIMARY KEY (id);
ALTER TABLE t_end.transport_scopes ADD CONSTRAINT transport_scopes_name_key UNIQUE (name);
CREATE UNLOGGED TABLE t_end.meshmapper_channel_members WITH (autovacuum_enabled = false) AS SELECT * FROM public.meshmapper_channel_members;
ALTER TABLE t_end.meshmapper_channel_members ADD CONSTRAINT meshmapper_channel_members_pkey PRIMARY KEY (iata, key_fingerprint);
CREATE UNLOGGED TABLE t_end.region_iatas WITH (autovacuum_enabled = false) AS SELECT * FROM public.region_iatas;
ALTER TABLE t_end.region_iatas ADD CONSTRAINT region_iatas_pkey PRIMARY KEY (region_id, iata);
CREATE UNLOGGED TABLE t_end.trace_tags WITH (autovacuum_enabled = false) AS SELECT * FROM public.trace_tags;
ALTER TABLE t_end.trace_tags ADD CONSTRAINT trace_tags_pkey PRIMARY KEY (trace_tag);
CREATE INDEX idx_trace_tags_last_heard ON t_end.trace_tags (last_heard_at DESC);
CREATE INDEX idx_trace_tags_keyset ON t_end.trace_tags (date_trunc('milliseconds', last_heard_at, 'UTC') DESC, trace_tag DESC);

-- packet_observations, in public's physical order.
CREATE UNLOGGED TABLE t_end.packet_observations (LIKE public.packet_observations INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
  WITH (autovacuum_enabled = false);
INSERT INTO t_end.packet_observations SELECT * FROM public.packet_observations;
ALTER TABLE t_end.packet_observations ADD CONSTRAINT packet_observations_pkey PRIMARY KEY (id);
ALTER TABLE t_end.packet_observations ADD CONSTRAINT packet_observations_packet_hash_observer_id_key UNIQUE (packet_hash, observer_id);
CREATE INDEX idx_observations_heard ON t_end.packet_observations USING btree (heard_at);
CREATE INDEX idx_observations_iata_heard ON t_end.packet_observations USING btree (iata, heard_at DESC);
CREATE INDEX idx_observations_observer ON t_end.packet_observations USING btree (observer_id, heard_at DESC);
CREATE INDEX idx_observations_adverts ON t_end.packet_observations (observer_id, id) WHERE payload_type = 4;
CREATE INDEX idx_observations_route_evidence ON t_end.packet_observations USING btree (iata, hash_size, decode(md5(path_bytes), 'hex'::text), heard_at DESC, id DESC)
  WHERE ((path_bytes IS NOT NULL) AND (payload_type IS NOT NULL) AND (payload_type <> 9) AND (hop_count >= 2));
CREATE INDEX idx_observations_packet ON t_end.packet_observations USING btree (packet_hash);
ALTER TABLE t_end.packet_observations ADD CONSTRAINT packet_observations_packet_hash_fkey
  FOREIGN KEY (packet_hash) REFERENCES t_end.packets (packet_hash) ON DELETE CASCADE;
ALTER TABLE t_end.packet_observations ADD CONSTRAINT packet_observations_observer_id_fkey
  FOREIGN KEY (observer_id) REFERENCES t_end.observers (id);

-- channel_messages, in public's physical order, checked equal to public both ways.
CREATE UNLOGGED TABLE t_end.channel_messages (LIKE public.channel_messages INCLUDING DEFAULTS)
  WITH (autovacuum_enabled = false);
INSERT INTO t_end.channel_messages SELECT * FROM public.channel_messages;
ALTER TABLE t_end.channel_messages ADD CONSTRAINT channel_messages_pkey PRIMARY KEY (id);
ALTER TABLE t_end.channel_messages ADD CONSTRAINT channel_messages_packet_hash_key UNIQUE (packet_hash);
CREATE INDEX idx_channel_messages_channel_id ON t_end.channel_messages USING btree (channel_id, id DESC);
CREATE INDEX idx_channel_messages_channel ON t_end.channel_messages USING btree (channel_id, sent_at DESC);
CREATE INDEX idx_channel_messages_sent_brin ON t_end.channel_messages USING brin (sent_at);
ALTER TABLE t_end.channel_messages ADD CONSTRAINT channel_messages_packet_hash_fkey
  FOREIGN KEY (packet_hash) REFERENCES t_end.packets (packet_hash) ON DELETE CASCADE;
DO $$
BEGIN
  IF EXISTS (SELECT * FROM t_end.channel_messages EXCEPT ALL SELECT * FROM public.channel_messages)
     OR EXISTS (SELECT * FROM public.channel_messages EXCEPT ALL SELECT * FROM t_end.channel_messages) THEN
    RAISE EXCEPTION 't_end.channel_messages differs from public.channel_messages';
  END IF;
END $$;

-- channel_message_iatas, filled as the backfill fills it.
CREATE UNLOGGED TABLE t_end.channel_message_iatas (
    message_id bigint NOT NULL REFERENCES t_end.channel_messages(id) ON DELETE CASCADE,
    iata character(3) NOT NULL,
    channel_id integer NOT NULL,
    scope_id integer,
    PRIMARY KEY (message_id, iata)
) WITH (autovacuum_enabled = false);
INSERT INTO t_end.channel_message_iatas (message_id, iata, channel_id, scope_id)
SELECT DISTINCT cm.id, po.iata, cm.channel_id, p.scope_id
FROM t_end.channel_messages cm
JOIN t_end.packets p ON p.packet_hash = cm.packet_hash
JOIN t_end.packet_observations po ON po.packet_hash = cm.packet_hash;
CREATE INDEX idx_cmi_channel_site ON t_end.channel_message_iatas USING btree (channel_id, iata, message_id DESC);
CREATE INDEX idx_cmi_site ON t_end.channel_message_iatas USING btree (iata, message_id DESC);
CREATE INDEX idx_cmi_channel_site_scope ON t_end.channel_message_iatas USING btree (channel_id, iata, scope_id, message_id DESC) WHERE (scope_id IS NOT NULL);
CREATE INDEX idx_cmi_channel_scope ON t_end.channel_message_iatas USING btree (channel_id, scope_id, message_id DESC) WHERE (scope_id IS NOT NULL);
CREATE INDEX idx_cmi_scope ON t_end.channel_message_iatas USING btree (scope_id, message_id DESC) WHERE (scope_id IS NOT NULL);

-- packet_sites: one row per packet and site at its newest hearing there, in ingest order.
CREATE UNLOGGED TABLE t_end.packet_sites (
    packet_hash    bytea        NOT NULL REFERENCES t_end.packets(packet_hash) ON DELETE CASCADE,
    iata           character(3) NOT NULL,
    site_heard_at  timestamptz  NOT NULL,
    first_heard_at timestamptz  NOT NULL,
    payload_type   smallint     NOT NULL CHECK (payload_type BETWEEN 0 AND 15),
    route_type     smallint     NOT NULL CHECK (route_type BETWEEN 0 AND 3),
    scope_id       integer
) WITH (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_cost_limit = 1000, autovacuum_enabled = false);
ALTER TABLE t_end.packet_sites ALTER COLUMN iata SET STATISTICS 500;
INSERT INTO t_end.packet_sites
SELECT po.packet_hash, po.iata, max(po.heard_at), p.first_heard_at, p.payload_type, p.route_type, p.scope_id
FROM t_end.packet_observations po JOIN t_end.packets p ON p.packet_hash = po.packet_hash
GROUP BY po.packet_hash, po.iata, p.first_heard_at, p.payload_type, p.route_type, p.scope_id
ORDER BY min(po.id);
ALTER TABLE t_end.packet_sites ADD CONSTRAINT packet_sites_pkey PRIMARY KEY (packet_hash, iata) INCLUDE (site_heard_at);
CREATE INDEX idx_packet_sites_site ON t_end.packet_sites USING btree (iata, site_heard_at DESC, packet_hash DESC) INCLUDE (payload_type, route_type, first_heard_at);
CREATE INDEX idx_packet_sites_site_type ON t_end.packet_sites USING btree (iata, payload_type, route_type, site_heard_at DESC, packet_hash DESC);
CREATE INDEX idx_packet_sites_site_scope ON t_end.packet_sites USING btree (iata, scope_id, site_heard_at DESC, packet_hash DESC) INCLUDE (payload_type, route_type, first_heard_at) WHERE (scope_id IS NOT NULL);

-- background_state: the read models complete and verified, observation payload types done.
CREATE UNLOGGED TABLE t_end.background_state (key text PRIMARY KEY, value jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
INSERT INTO t_end.background_state (key, value)
SELECT 'cmi', jsonb_build_object('backfill_max_id', m.hi, 'backfilled_to_id', m.hi, 'complete', true,
         'obs_prev_hi', o.hi, 'obs_hi', o.hi, 'msg_prev_hi', m.hi, 'msg_hi', m.hi,
         'first_verified_at', to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'))
FROM (SELECT coalesce(max(id), 0) hi FROM t_end.channel_messages) m, (SELECT coalesce(max(id), 0) hi FROM t_end.packet_observations) o;
INSERT INTO t_end.background_state (key, value)
SELECT 'packet_sites', jsonb_build_object('writer_from_id', o.hi, 'backfilled_down_to_id', 0, 'complete', true,
         'recon_prev_hi', o.hi, 'recon_hi', o.hi,
         'first_verified_at', to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'))
FROM (SELECT coalesce(max(id), 0) hi FROM t_end.packet_observations) o;
INSERT INTO t_end.background_state (key, value)
SELECT 'observation_payload_type', jsonb_build_object('next_id', o.hi + 1, 'done', true)
FROM (SELECT coalesce(max(id), 0) hi FROM t_end.packet_observations) o;

-- Statistics, then the stale site, then the visibility map (background_state, three rows, is not checked: a
-- snapshot open elsewhere can keep its one page from being all-visible, and no plan depends on it).
VACUUM ANALYZE t_end.packets;
VACUUM ANALYZE t_end.nodes;
VACUUM ANALYZE t_end.node_iatas;
VACUUM ANALYZE t_end.observers;
VACUUM ANALYZE t_end.packet_observations;
VACUUM ANALYZE t_end.channel_messages;
VACUUM ANALYZE t_end.channel_message_iatas;
VACUUM ANALYZE t_end.packet_sites;
VACUUM ANALYZE t_end.background_state;
VACUUM ANALYZE t_end.channel_iatas, t_end.node_neighbors, t_end.transport_scopes, t_end.meshmapper_channel_members,
  t_end.region_iatas, t_end.trace_tags;
INSERT INTO t_end.packet_sites
SELECT ps.packet_hash, 'NEW', ps.site_heard_at, ps.first_heard_at, ps.payload_type, ps.route_type, ps.scope_id
FROM t_end.packet_sites ps
WHERE ps.iata = (SELECT po.iata FROM t_end.packet_observations po
                 WHERE po.id > (SELECT max(id) - 200000 FROM t_end.packet_observations)
                 GROUP BY po.iata ORDER BY count(*) DESC, po.iata OFFSET 9 LIMIT 1)
ORDER BY ps.site_heard_at;
VACUUM t_end.packets;
VACUUM t_end.known_routes;
VACUUM t_end.nodes;
VACUUM t_end.node_iatas;
VACUUM t_end.observers;
VACUUM t_end.packet_observations;
VACUUM t_end.background_state;
VACUUM t_end.channel_iatas, t_end.node_neighbors, t_end.transport_scopes, t_end.meshmapper_channel_members,
  t_end.region_iatas, t_end.trace_tags;
VACUUM ANALYZE t_end.channel_messages;
VACUUM ANALYZE t_end.channel_message_iatas;
VACUUM t_end.packet_sites;
VACUUM t_end.packet_sites;
DO $$
DECLARE r record;
BEGIN
  FOR r IN SELECT relname FROM pg_class
           WHERE relnamespace = 't_end'::regnamespace AND relkind = 'r' AND relname <> 'background_state'
             AND relpages > 0 AND relallvisible < relpages LOOP
    RAISE EXCEPTION 't_end.%: the visibility map is not set on every page', r.relname;
  END LOOP;
END $$;
SELECT c.relname, c.relkind, pg_size_pretty(pg_relation_size(c.oid)) AS size, c.reltuples::bigint
FROM pg_class c WHERE c.relnamespace = 't_end'::regnamespace AND c.relkind IN ('r', 'i') ORDER BY 1;
SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) AS t_end_total
FROM pg_class c WHERE c.relnamespace = 't_end'::regnamespace AND c.relkind = 'r';
