-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Superseded by end_state.sql for the local tier; kept as the evidence of its change and for its diff cases.
-- Overlay for the local-copy tier of the channel_message_iatas group, diff/channel_message_iatas.yaml and
-- storediff/channel_message_iatas.yaml: it builds t_s4a; it never writes public. It holds
--   t_s4a.channel_messages       a copy of public's in its physical order, with every index public has plus
--                                idx_channel_messages_channel_id, checked equal to public both ways;
--   t_s4a.channel_message_iatas  the migrations' table and indexes, foreign key to the copy, filled as the
--                                backfill fills it;
--   t_s4a.background_state       only the cmi state, complete and verified, so a read-only server trusts the copy.
-- Statements run with search_path "t_s4a, public", so these three resolve here and every other table to public.
-- VACUUM ANALYZE runs twice: a snapshot still open during the first leaves the visibility map unset.
--
-- The group's resolvers read channel_message_iatas too, and resolvers run on the run's base search_path, so run its
-- local tier with that path:
--   planguard run --tier local --only '^cmi[.]' --search-path 't_s4a, public' --dsn ...
--
-- Local copy only. Remove it with
--   DROP SCHEMA t_s4a CASCADE;
-- Run: psql "$DSN" -v ON_ERROR_STOP=1 -f db/planguard/overlay/channel_message_iatas.sql
\set ON_ERROR_STOP 1
\timing on
SET maintenance_work_mem = '1GB';
SET jit = off;
CREATE SCHEMA IF NOT EXISTS t_s4a;
DROP TABLE IF EXISTS t_s4a.channel_message_iatas, t_s4a.channel_messages, t_s4a.background_state;

CREATE TABLE t_s4a.channel_messages (LIKE public.channel_messages INCLUDING DEFAULTS);
INSERT INTO t_s4a.channel_messages SELECT * FROM public.channel_messages;
ALTER TABLE t_s4a.channel_messages ADD CONSTRAINT channel_messages_pkey PRIMARY KEY (id);
ALTER TABLE t_s4a.channel_messages ADD CONSTRAINT channel_messages_packet_hash_key UNIQUE (packet_hash);
CREATE INDEX idx_channel_messages_channel ON t_s4a.channel_messages USING btree (channel_id, sent_at DESC);
CREATE INDEX idx_channel_messages_sent_brin ON t_s4a.channel_messages USING brin (sent_at);
CREATE INDEX idx_channel_messages_channel_id ON t_s4a.channel_messages USING btree (channel_id, id DESC);
DO $$
BEGIN
  IF EXISTS (SELECT * FROM t_s4a.channel_messages EXCEPT ALL SELECT * FROM public.channel_messages)
     OR EXISTS (SELECT * FROM public.channel_messages EXCEPT ALL SELECT * FROM t_s4a.channel_messages) THEN
    RAISE EXCEPTION 't_s4a.channel_messages differs from public.channel_messages';
  END IF;
END $$;

CREATE TABLE t_s4a.channel_message_iatas (
    message_id bigint NOT NULL REFERENCES t_s4a.channel_messages(id) ON DELETE CASCADE,
    iata character(3) NOT NULL,
    channel_id integer NOT NULL,
    scope_id integer,
    PRIMARY KEY (message_id, iata)
);
INSERT INTO t_s4a.channel_message_iatas (message_id, iata, channel_id, scope_id)
SELECT DISTINCT cm.id, po.iata, cm.channel_id, p.scope_id
FROM t_s4a.channel_messages cm
JOIN public.packets p ON p.packet_hash = cm.packet_hash
JOIN public.packet_observations po ON po.packet_hash = cm.packet_hash;
CREATE INDEX idx_cmi_channel_site ON t_s4a.channel_message_iatas USING btree (channel_id, iata, message_id DESC);
CREATE INDEX idx_cmi_site ON t_s4a.channel_message_iatas USING btree (iata, message_id DESC);
CREATE INDEX idx_cmi_channel_site_scope ON t_s4a.channel_message_iatas USING btree (channel_id, iata, scope_id, message_id DESC) WHERE (scope_id IS NOT NULL);
CREATE INDEX idx_cmi_channel_scope ON t_s4a.channel_message_iatas USING btree (channel_id, scope_id, message_id DESC) WHERE (scope_id IS NOT NULL);
CREATE INDEX idx_cmi_scope ON t_s4a.channel_message_iatas USING btree (scope_id, message_id DESC) WHERE (scope_id IS NOT NULL);

CREATE TABLE t_s4a.background_state (key text PRIMARY KEY, value jsonb NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
INSERT INTO t_s4a.background_state (key, value)
SELECT 'cmi', jsonb_build_object('backfill_max_id', m.hi, 'backfilled_to_id', m.hi, 'complete', true,
         'obs_prev_hi', o.hi, 'obs_hi', o.hi, 'msg_prev_hi', m.hi, 'msg_hi', m.hi,
         'first_verified_at', to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"'))
FROM (SELECT coalesce(max(id), 0) hi FROM t_s4a.channel_messages) m, (SELECT coalesce(max(id), 0) hi FROM public.packet_observations) o;

VACUUM ANALYZE t_s4a.channel_messages;
VACUUM ANALYZE t_s4a.channel_message_iatas;
VACUUM ANALYZE t_s4a.channel_messages;
VACUUM ANALYZE t_s4a.channel_message_iatas;
SELECT c.relname, pg_size_pretty(pg_total_relation_size(c.oid)) FROM pg_class c
WHERE c.relnamespace = 't_s4a'::regnamespace AND c.relkind = 'r' ORDER BY 1;
