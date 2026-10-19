-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Fixture step 20: known_routes at 1/50.
--   * Region sizes: routes_regions (production's region skew). Regions with a (region, hop) histogram in
--     routes_iata_hop take their hop counts from it, each non-empty cell keeping at least one route, so rare and
--     single-route hops stay present; the other regions draw hops from the pooled histogram.
--   * Age and observation_count: routes_age_obs / routes_obs, from the copy's clock (2026-10-18 19:00 UTC);
--     3 % of last_seen values are whole seconds, so cursor ties exist.
--   * Relays: drawn from the region's relay nodes (node_iatas, types 2 and 3) with the densest region's
--     popularity curve (routes_relay) folded onto the pool, so a few relays are on many routes and most on few.
--   * WRE gets no routes; NEW is loaded after ANALYZE (step 90).
SELECT setseed(0.43);

SELECT planguard_src.mkcum('(SELECT hop_count, sum(n) n FROM planguard_src.routes_iata_hop GROUP BY 1) s', 'hop_count', 'n', 'c_routes_hop');
SELECT planguard_src.mkcum('(SELECT age_h, sum(n) n FROM planguard_src.routes_age_obs GROUP BY 1) s', 'age_h', 'n', 'c_routes_age');
SELECT planguard_src.mkcum('(SELECT obs, n FROM planguard_src.routes_obs WHERE ge3) s', 'obs', 'n', 'c_routes_obs_ge3');
SELECT planguard_src.mkcum('(SELECT obs, n FROM planguard_src.routes_obs WHERE NOT ge3) s', 'obs', 'n', 'c_routes_obs_lt3');
SELECT planguard_src.mkcum('planguard_src.routes_relay', 'rank', 'n', 'c_routes_relay');
CREATE TABLE planguard_src.routes_age_p AS
SELECT age_h, sum(n) FILTER (WHERE obs_ge3)::float8 / sum(n) AS p FROM planguard_src.routes_age_obs GROUP BY 1;

-- Relay pools: idx 1..k per region, in a fixed pseudo-random order.
CREATE TABLE planguard_src.region_relays AS
SELECT ni.iata, row_number() OVER (PARTITION BY ni.iata ORDER BY md5(ni.iata || n.id::text)) AS idx, n.id, n.public_key
FROM node_iatas ni JOIN nodes n ON n.id = ni.node_id AND n.node_type IN (2, 3);
CREATE TABLE planguard_src.region_relay_k AS SELECT iata, count(*)::int AS k FROM planguard_src.region_relays GROUP BY 1;
CREATE UNIQUE INDEX ON planguard_src.region_relays (iata, idx);

-- One row per route: region and hop count.
CREATE TABLE planguard_src.kr_cells AS
SELECT h.iata, h.hop_count AS hop, greatest(1, round(h.n / 50.0))::int AS n
FROM planguard_src.routes_iata_hop h WHERE h.n > 0 AND h.iata NOT IN ('WRE', 'NEW');
CREATE TABLE planguard_src.kr0 AS
SELECT row_number() OVER (ORDER BY c.iata, c.hop, g) AS rid, c.iata, c.hop
FROM planguard_src.kr_cells c CROSS JOIN LATERAL generate_series(1, c.n) g;
INSERT INTO planguard_src.kr0
SELECT (SELECT max(rid) FROM planguard_src.kr0) + row_number() OVER (ORDER BY r.iata, g), r.iata,
       (SELECT array_agg(v ORDER BY lo) FROM planguard_src.c_routes_hop)[width_bucket(random(), (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.c_routes_hop))]
FROM planguard_src.routes_regions r CROSS JOIN LATERAL generate_series(1, greatest(1, round(r.prod_rows / 50.0))::int) g
WHERE r.iata NOT IN ('WRE', 'NEW') AND r.iata NOT IN (SELECT iata FROM planguard_src.routes_iata_hop);

-- Ages, observation counts, sightings. The draws are stored first, so each is made once per route.
CREATE TABLE planguard_src.kra AS
SELECT k.*, (SELECT array_agg(v ORDER BY lo) FROM planguard_src.c_routes_age)[width_bucket(random(), (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.c_routes_age))] AS age_h,
       random() AS r_obs, random() AS r_sub, random() AS r_tie, random() AS r_o2, random() AS r_fs, random() AS r_hs
FROM planguard_src.kr0 k;
CREATE TABLE planguard_src.kr1 AS
SELECT a.rid, a.iata, a.hop, a.age_h,
       CASE WHEN a.age_h >= 168 OR a.r_obs < coalesce(ap.p, 1)
            THEN (SELECT array_agg(v ORDER BY lo) FROM planguard_src.c_routes_obs_ge3)[width_bucket(a.r_o2, (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.c_routes_obs_ge3))]
            ELSE (SELECT array_agg(v ORDER BY lo) FROM planguard_src.c_routes_obs_lt3)[width_bucket(a.r_o2, (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.c_routes_obs_lt3))] END AS obs,
       CASE WHEN a.r_tie < 0.03 THEN date_trunc('second', timestamptz '2026-10-18 19:00+00' - (a.age_h + a.r_sub) * interval '1 hour')
            ELSE timestamptz '2026-10-18 19:00+00' - (a.age_h + a.r_sub) * interval '1 hour' END AS ls,
       a.r_fs, a.r_hs
FROM planguard_src.kra a
LEFT JOIN planguard_src.routes_age_p ap ON ap.age_h = a.age_h;

-- Relays per hop, folded from the densest region's popularity rank onto the region's pool.
CREATE TABLE planguard_src.kr_slots AS
SELECT s.rid, s.j, rr.id, rr.public_key
FROM (SELECT k.rid, g.j, k.iata,
             1 + floor((width_bucket(random(), (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.c_routes_relay)) - 1)::float8
                       * pk.k / (SELECT count(*) FROM planguard_src.c_routes_relay))::int AS idx
      FROM planguard_src.kr0 k JOIN planguard_src.region_relay_k pk ON pk.iata = k.iata
      CROSS JOIN LATERAL generate_series(1, k.hop) g(j)) s
JOIN planguard_src.region_relays rr ON rr.iata = s.iata AND rr.idx = s.idx;
CREATE TABLE planguard_src.kr_paths AS
SELECT s.rid, array_agg(s.id ORDER BY s.j) AS node_ids,
       array_agg(substr(s.public_key, 1, CASE WHEN k.r_hs < 0.006 THEN 1 WHEN k.r_hs < 0.76 THEN 2 ELSE 3 END) ORDER BY s.j) AS hash_prefix
FROM planguard_src.kr_slots s JOIN planguard_src.kr1 k ON k.rid = s.rid GROUP BY s.rid;

-- Ids follow first sighting; the heap is in (iata, path_key) order.
INSERT INTO known_routes (id, path_key, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count, last_reconfirmed_at)
SELECT 1000 + row_number() OVER (ORDER BY fs, pk), pk, p.node_ids, p.hash_prefix, k.iata, k.hop, fs, k.ls, k.obs, k.ls
FROM (SELECT kr1.*, decode(md5('route' || kr1.rid), 'hex') AS pk,
             greatest(timestamptz '2026-09-01 00:00+00', least(kr1.ls, kr1.ls - (kr1.r_fs * kr1.r_fs * 10) * interval '1 day')) AS fs
      FROM planguard_src.kr1) k
JOIN planguard_src.kr_paths p ON p.rid = k.rid
ORDER BY k.iata, k.pk;
SELECT setval('known_routes_new_id_seq', (SELECT max(id) FROM known_routes));
DROP TABLE planguard_src.kra, planguard_src.kr_slots, planguard_src.kr_paths;
