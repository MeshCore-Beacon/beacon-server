-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Fixture step 30: packets at 1/20 over the 72 hours before 2026-10-18 17:00 UTC: hour, (payload, route) type,
-- the channel / origin / trace / scope shares of each type, the scope skew, observation counts and life times as
-- in the testdata. 2 % of last_heard_at values are whole seconds. r_q is the packet's quantile of observation
-- count; step 40 draws its site count at the same quantile, so the most heard packets reach the most sites. The packets are staged in planguard_src.pk:
-- steps 40 and 50 correct them, and step 60 inserts them once, so the table holds no dead rows.
SELECT setseed(0.44);

SELECT planguard_src.mkcum('planguard_src.packets_hour', 'age_h', 'n', 'c_packets_hour');
SELECT planguard_src.mkcum('(SELECT payload_type * 4 + route_type AS k, n FROM planguard_src.packets_types) s', 'k', 'n', 'c_packets_types');
SELECT planguard_src.mkcum('planguard_src.packets_scope', 'rank', 'n', 'c_packets_scope');
SELECT planguard_src.mkcum('planguard_src.packets_obscount', 'obs', 'n', 'c_packets_obscount');

CREATE TABLE planguard_src.pk0 AS
SELECT i, r_q,
       (SELECT array_agg(v ORDER BY lo) FROM planguard_src.c_packets_hour)[width_bucket(random(), (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.c_packets_hour))] AS age_h,
       random() AS r_sub,
       (SELECT array_agg(v ORDER BY lo) FROM planguard_src.c_packets_types)[width_bucket(random(), (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.c_packets_types))] AS k,
       floor(random() * 100)::int AS life_p, random() AS r_life, random() AS r_ch, random() AS r_or, random() AS r_tr, random() AS r_sc,
       (SELECT array_agg(v ORDER BY lo) FROM planguard_src.c_packets_obscount)[width_bucket(r_q, (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.c_packets_obscount))] AS obs,
       (SELECT array_agg(v ORDER BY lo) FROM planguard_src.c_packets_scope)[width_bucket(random(), (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.c_packets_scope))] AS scope_rank,
       1 + floor(random() * (SELECT count(*) FROM nodes))::int AS origin_k,
       1 + floor(random() * 729)::int AS channel_k,
       random() AS r_tie
FROM (SELECT i, random() AS r_q FROM generate_series(1, (SELECT round(sum(n) / 20.0)::int FROM planguard_src.packets_hour)) i) g;

CREATE TABLE planguard_src.node_keys AS SELECT row_number() OVER (ORDER BY id) AS k, public_key FROM nodes;
CREATE UNIQUE INDEX ON planguard_src.node_keys (k);

CREATE TABLE planguard_src.pk AS
SELECT substr(decode(md5('packet' || p.i), 'hex'), 1, 8) AS packet_hash, (p.k / 4)::smallint AS payload_type,
       0::smallint AS payload_version, (p.k % 4)::smallint AS route_type,
       p.r_sc < t.n_scoped::float8 / t.n AS transport_codes_present,
       CASE WHEN p.r_sc < t.n_scoped::float8 / t.n THEN p.scope_rank END AS scope_id,
       CASE WHEN p.r_or < t.n_origin::float8 / t.n THEN nk.public_key END AS origin_pubkey,
       decode(md5('raw' || p.i) || md5('raw2' || p.i) || md5('raw3' || p.i), 'hex') AS raw_payload,
       CASE WHEN p.r_ch < t.n_channel::float8 / t.n THEN c.channel_hash END AS channel_hash,
       CASE WHEN p.r_tr < t.n_trace::float8 / t.n THEN substr(decode(md5('trace' || p.i), 'hex'), 1, 4) END AS trace_tag,
       p.lh - make_interval(secs => greatest(0, l0.life_s + (l1.life_s - l0.life_s) * p.r_life)) AS first_heard_at,
       p.lh AS last_heard_at, p.obs::bigint AS observation_count, p.r_q
FROM (SELECT pk0.*, CASE WHEN pk0.r_tie < 0.02 THEN date_trunc('second', pk0.t) ELSE pk0.t END AS lh
      FROM (SELECT pk0.*, timestamptz '2026-10-18 17:00+00' - (pk0.age_h + pk0.r_sub) * interval '1 hour' AS t
            FROM planguard_src.pk0) pk0) p
JOIN planguard_src.packets_types t ON t.payload_type * 4 + t.route_type = p.k
JOIN planguard_src.packets_life l0 ON l0.p = p.life_p
JOIN planguard_src.packets_life l1 ON l1.p = p.life_p + 1
JOIN planguard_src.node_keys nk ON nk.k = p.origin_k
JOIN planguard_src.ch c ON c.id = p.channel_k;
CREATE UNIQUE INDEX ON planguard_src.pk (packet_hash);
