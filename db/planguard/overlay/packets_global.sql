-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Overlay for the local-copy tier of the packets_global group and for diff/packets_global.yaml, built in the schema
-- t_s5a from public:
--   packets     with the index set it has once the old single-column indexes are dropped (migrations 025-033 plus
--               the indexes the table keeps);
--   nodes, node_iatas, observers
--               with the index set and fillfactor of migrations 011-024, for a copy taken before them.
-- Statements run with search_path "t_s5a, public", so these tables resolve here and every other table to public.
--
-- It reads public and writes only inside t_s5a (about 860 MB on a full copy, most of it packets). Remove it with
--   DROP SCHEMA t_s5a CASCADE;
-- Run: psql "$DSN" -v ON_ERROR_STOP=1 -f db/planguard/overlay/packets_global.sql
--
-- UNLOGGED, and autovacuum is off so the statistics stay as built. parsed_payload is not copied (NULL): it only
-- feeds the projection's summary text, so plans and buffer counts (one heap page per projected packet) are the
-- same, but result rows differ in summary; compare API results against public instead.
\set ON_ERROR_STOP 1
\timing on
SET maintenance_work_mem = '512MB';
SET jit = off;
CREATE SCHEMA IF NOT EXISTS t_s5a;
DROP TABLE IF EXISTS t_s5a.packets;
CREATE UNLOGGED TABLE t_s5a.packets (LIKE public.packets INCLUDING DEFAULTS INCLUDING CONSTRAINTS)
  WITH (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_cost_limit = 1000, autovacuum_enabled = false);
INSERT INTO t_s5a.packets
SELECT packet_hash, payload_type, payload_version, route_type, transport_codes_present, region_code, sub_region_code, scope_id,
       origin_pubkey, raw_payload, raw_header, NULL::jsonb, decrypted, channel_hash, trace_tag, first_heard_at, last_heard_at, observation_count
FROM public.packets;
ALTER TABLE t_s5a.packets ADD CONSTRAINT packets_pkey PRIMARY KEY (packet_hash);
ALTER TABLE t_s5a.packets ADD CONSTRAINT packets_payload_type_domain CHECK (payload_type BETWEEN 0 AND 15);
ALTER TABLE t_s5a.packets ADD CONSTRAINT packets_route_type_domain CHECK (route_type BETWEEN 0 AND 3);
CREATE INDEX idx_packets_last_heard_hash ON t_s5a.packets (last_heard_at DESC, packet_hash DESC);
CREATE INDEX idx_packets_type_last_heard ON t_s5a.packets (payload_type, route_type, last_heard_at DESC, packet_hash DESC);
CREATE INDEX idx_packets_scope_last_heard ON t_s5a.packets (scope_id, last_heard_at DESC, packet_hash DESC)
  INCLUDE (payload_type, route_type, first_heard_at) WHERE scope_id IS NOT NULL;
CREATE INDEX idx_packets_first_heard_brin ON t_s5a.packets USING brin (first_heard_at);
CREATE INDEX idx_packets_origin ON t_s5a.packets (origin_pubkey, first_heard_at DESC) WHERE origin_pubkey IS NOT NULL;
CREATE INDEX idx_packets_channel ON t_s5a.packets (channel_hash, first_heard_at DESC) WHERE channel_hash IS NOT NULL;
CREATE INDEX idx_packets_trace_tag ON t_s5a.packets (trace_tag) WHERE trace_tag IS NOT NULL;
CREATE INDEX idx_packets_heard_inverted ON t_s5a.packets (first_heard_at)
  INCLUDE (last_heard_at, packet_hash, payload_type, route_type, scope_id) WHERE (first_heard_at > last_heard_at) IS TRUE;
-- Created before VACUUM ANALYZE so the extended statistics are collected.
CREATE STATISTICS t_s5a.packets_heard_inverted_stat ON ((first_heard_at > last_heard_at)) FROM t_s5a.packets;
VACUUM ANALYZE t_s5a.packets;
-- nodes, node_iatas and observers: every public index and key except the ones migrations 011-024 drop, node_iatas'
-- (iata) index in place of (iata, last_heard DESC), and observers at fillfactor 50.
DO $$
DECLARE
  t record; ix record; c record;
  drops text[] := ARRAY['idx_nodes_type_last_seen', 'idx_nodes_pubkey', 'idx_nodes_location', 'idx_nodes_min_firmware',
    'idx_nodes_multibyte_paths', 'idx_nodes_multibyte_traces', 'idx_observers_last_seen', 'idx_observers_pubkey',
    'idx_observers_type', 'idx_node_iatas_iata'];
BEGIN
  FOR t IN SELECT * FROM (VALUES ('nodes', 100), ('node_iatas', 100), ('observers', 50)) v(name, ff) LOOP
    EXECUTE format('DROP TABLE IF EXISTS t_s5a.%I', t.name);
    EXECUTE format('CREATE UNLOGGED TABLE t_s5a.%I (LIKE public.%I INCLUDING DEFAULTS INCLUDING CONSTRAINTS) '
                   'WITH (fillfactor = %s, autovacuum_enabled = false)', t.name, t.name, t.ff);
    EXECUTE format('INSERT INTO t_s5a.%I SELECT * FROM public.%I', t.name, t.name);
    FOR c IN SELECT conname, pg_get_constraintdef(oid) AS def FROM pg_constraint
             WHERE conrelid = format('public.%I', t.name)::regclass AND contype IN ('p', 'u') LOOP
      EXECUTE format('ALTER TABLE t_s5a.%I ADD CONSTRAINT %I %s', t.name, c.conname, c.def);
    END LOOP;
    FOR ix IN SELECT i.indexname, i.indexdef FROM pg_indexes i
              WHERE i.schemaname = 'public' AND i.tablename = t.name
                AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conname = i.indexname
                                AND k.conrelid = format('public.%I', t.name)::regclass) LOOP
      IF ix.indexname = ANY (drops) THEN CONTINUE; END IF;
      EXECUTE replace(ix.indexdef, ' ON public.', ' ON t_s5a.');
    END LOOP;
  END LOOP;
END $$;
CREATE INDEX idx_node_iatas_iata ON t_s5a.node_iatas (iata);
VACUUM ANALYZE t_s5a.nodes;
VACUUM ANALYZE t_s5a.node_iatas;
VACUUM ANALYZE t_s5a.observers;
SELECT c.relname, pg_size_pretty(pg_relation_size(c.oid)) FROM pg_class c WHERE c.relnamespace = 't_s5a'::regnamespace ORDER BY 1;
SELECT tablename, indexname FROM pg_indexes WHERE schemaname = 't_s5a' ORDER BY 1, 2;
SELECT count(*) AS inverted_rows FROM t_s5a.packets WHERE (first_heard_at > last_heard_at) IS TRUE;
SELECT pg_size_pretty(pg_total_relation_size('t_s5a.packets')) AS t_s5a_packets_total;
