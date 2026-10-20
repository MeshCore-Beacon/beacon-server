-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Fixture step 90 (after ANALYZE): the stale region. NEW gets about 2,100 routes (1/50 of a region that grew
-- after the last ANALYZE) that the statistics have never seen, so the planner still estimates it at one row.
-- Autovacuum is off on known_routes, so nothing refreshes them. Hops: 4 is dense (28 %), 14 has a single
-- route, 1 has none; the rest follow the pooled histogram. Ages spread over the last 14 days.
ALTER TABLE known_routes SET (autovacuum_enabled = false);
SELECT setseed(0.49);

SELECT planguard_src.mkcum('(SELECT hop_count, sum(n) n FROM planguard_src.routes_iata_hop WHERE hop_count NOT IN (1, 4, 14) GROUP BY 1) s',
                           'hop_count', 'n', 'c_new_hop');
CREATE TABLE planguard_src.new0 AS
SELECT g AS rid,
       CASE WHEN g = 1 THEN 14 WHEN random() < 0.28 THEN 4
            ELSE (SELECT array_agg(v ORDER BY lo) FROM planguard_src.c_new_hop)[width_bucket(random(), (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.c_new_hop))] END AS hop,
       timestamptz '2026-10-18 19:00+00' - random() * interval '14 days' AS ls, 1 + floor(random() * random() * 40)::int AS obs, random() AS r_hs
FROM generate_series(1, 2100) g;
CREATE TABLE planguard_src.new_paths AS
SELECT s.rid, array_agg(rr.id ORDER BY s.j) AS node_ids,
       array_agg(substr(rr.public_key, 1, CASE WHEN s.r_hs < 0.006 THEN 1 WHEN s.r_hs < 0.76 THEN 2 ELSE 3 END) ORDER BY s.j) AS hash_prefix
FROM (SELECT n.rid, n.r_hs, g.j,
             1 + floor((width_bucket(random(), (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.c_routes_relay)) - 1)::float8
                       * (SELECT k FROM planguard_src.region_relay_k WHERE iata = 'NEW') / (SELECT count(*) FROM planguard_src.c_routes_relay))::int AS idx
      FROM planguard_src.new0 n CROSS JOIN LATERAL generate_series(1, n.hop) g(j)) s
JOIN planguard_src.region_relays rr ON rr.iata = 'NEW' AND rr.idx = s.idx
GROUP BY s.rid;
INSERT INTO known_routes (id, path_key, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count, last_reconfirmed_at)
SELECT (SELECT max(id) FROM known_routes) + row_number() OVER (ORDER BY n.ls, n.rid), decode(md5('new-route' || n.rid), 'hex'),
       p.node_ids, p.hash_prefix, 'NEW', n.hop, n.ls - interval '2 days', n.ls, n.obs, n.ls
FROM planguard_src.new0 n JOIN planguard_src.new_paths p ON p.rid = n.rid
ORDER BY decode(md5('new-route' || n.rid), 'hex');
SELECT setval('known_routes_new_id_seq', (SELECT max(id) FROM known_routes));
DROP TABLE planguard_src.new0, planguard_src.new_paths;

-- The stale site for channel messages: NEW heard every 20th of the newest 4,000 messages after the last ANALYZE,
-- so channel_message_iatas' statistics still estimate it at one row.
ALTER TABLE channel_message_iatas SET (autovacuum_enabled = false);
INSERT INTO channel_message_iatas (message_id, iata, channel_id, scope_id)
SELECT cm.id, 'NEW', cm.channel_id, p.scope_id
FROM (SELECT id, channel_id, packet_hash, row_number() OVER (ORDER BY id DESC) rn FROM channel_messages ORDER BY id DESC LIMIT 4000) cm
JOIN packets p ON p.packet_hash = cm.packet_hash
WHERE cm.rn % 20 = 0
ORDER BY cm.id;
