-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Fixture step 40: packet_observations, about observation_count rows per packet.
--   * Sites: each packet is heard at n_sites distinct sites (po_sites_per_packet at the packet's observation-count
--     quantile, at most its observation count), drawn by site volume (po_iata); its observations go round them.
--   * Observers: each observer hears at its home site only (po_observer), so a packet's observers at a site are
--     that site's, drawn by volume; an observer hears a packet once, so a small site caps the packet's count there.
--   * Times: the packet's last observation is at last_heard_at, the rest within its life, 1 % before
--     first_heard_at (late reports); po_whole_second percent of heard_at values are whole seconds.
-- Then each staged packet's observation_count and last_heard_at are set from its observations. The observations are
-- staged in planguard_src.po; step 60 inserts them.
SELECT setseed(0.45);

SELECT planguard_src.mkcum('planguard_src.po_iata', 'iata', 'n', 'c_po_iata');
SELECT planguard_src.mkcum('planguard_src.po_sites_per_packet', 'n_sites', 'packets', 'c_po_sites');
SELECT planguard_src.mkcum('planguard_src.po_hop', 'hop_count', 'n', 'c_po_hop');
-- Observers by site, in volume order: idx 1..cnt and the cumulative volume within the site.
CREATE TABLE planguard_src.site_observers AS
SELECT o.home_iata AS iata, row_number() OVER (PARTITION BY o.home_iata ORDER BY o.rank) AS idx,
       count(*) OVER (PARTITION BY o.home_iata) AS cnt, md5('observer' || o.rank)::uuid AS observer_id,
       sum(o.n) OVER (PARTITION BY o.home_iata ORDER BY o.rank ROWS UNBOUNDED PRECEDING)::float8 / sum(o.n) OVER (PARTITION BY o.home_iata) AS hi
FROM planguard_src.po_observer o WHERE o.n > 0;
CREATE INDEX ON planguard_src.site_observers (iata, hi);
CREATE UNIQUE INDEX ON planguard_src.site_observers (iata, idx);

-- Each packet's sites: n_sites distinct ones, drawn by volume without replacement (the first draws that differ).
CREATE TABLE planguard_src.pk_sites AS
SELECT d.packet_hash, (array_agg(d.iata ORDER BY d.first_draw))[1:max(d.n_sites)] AS sites
FROM (SELECT x.packet_hash, x.n_sites, x.iata, min(x.g) AS first_draw
      FROM (SELECT p.packet_hash, p.n_sites, g,
                   (SELECT array_agg(v ORDER BY lo) FROM planguard_src.c_po_iata)[width_bucket(random(), (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.c_po_iata))] AS iata
            FROM (SELECT packet_hash, greatest(1, least(observation_count::int,
                         (SELECT array_agg(v ORDER BY lo) FROM planguard_src.c_po_sites)[width_bucket(r_q, (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.c_po_sites))])) AS n_sites
                  FROM planguard_src.pk) p
            CROSS JOIN LATERAL generate_series(1, 3 * p.n_sites + 2) g) x
      GROUP BY x.packet_hash, x.n_sites, x.iata) d
GROUP BY d.packet_hash;

-- Observations: the packet's observation_count goes round its sites; at each site the packet is heard by distinct
-- observers, from one drawn by volume onwards in volume order, at most every observer of the site once.
CREATE TABLE planguard_src.po1 AS
SELECT q.packet_hash, q.payload_type, q.iata, so.observer_id, q.si = 1 AND t.t = 0 AS newest, random() AS r_t, random() AS r_skew,
       q.first_heard_at, q.last_heard_at,
       (SELECT array_agg(v ORDER BY lo) FROM planguard_src.c_po_hop)[width_bucket(random(), (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.c_po_hop))] AS hop
FROM (SELECT p.packet_hash, p.payload_type, p.first_heard_at, p.last_heard_at, si.si, s.sites[si.si] AS iata,
             (p.observation_count::int - si.si) / cardinality(s.sites) + 1 AS quota, st.idx AS start, st.cnt
      FROM planguard_src.pk p JOIN planguard_src.pk_sites s ON s.packet_hash = p.packet_hash
      CROSS JOIN LATERAL (SELECT g AS si, random() AS r
                          FROM generate_series(1, least(cardinality(s.sites), greatest(1, p.observation_count::int))) g) si
      CROSS JOIN LATERAL (SELECT so.idx, so.cnt FROM planguard_src.site_observers so
                          WHERE so.iata = s.sites[si.si] AND so.hi > si.r ORDER BY so.hi LIMIT 1) st) q
CROSS JOIN LATERAL generate_series(0, least(q.quota, q.cnt) - 1) t(t)
JOIN planguard_src.site_observers so ON so.iata = q.iata AND so.idx = 1 + (q.start - 1 + t.t) % q.cnt;

CREATE TABLE planguard_src.po AS
SELECT row_number() OVER (ORDER BY heard_at, packet_hash, observer_id) AS id, packet_hash, observer_id, iata, heard_at,
       hop AS path_length_byte, 1::smallint AS hash_size, hop AS hop_count,
       substr(decode(md5(packet_hash::text || observer_id::text) || md5('path' || packet_hash::text), 'hex'), 1, hop) AS path_bytes,
       (-60 - floor(random() * 60))::smallint AS rssi, (random() * 20 - 8)::real AS snr, payload_type
FROM (SELECT y.*, CASE WHEN random() * 100 < (SELECT whole_second_pct FROM planguard_src.po_whole_second LIMIT 1)
                       THEN date_trunc('second', y.t) ELSE y.t END AS heard_at
      FROM (SELECT po1.*, CASE WHEN po1.newest THEN po1.last_heard_at
                               WHEN po1.r_skew < 0.01 THEN po1.first_heard_at - po1.r_t * interval '30 min'
                               ELSE po1.first_heard_at + (po1.last_heard_at - po1.first_heard_at) * po1.r_t END AS t
            FROM planguard_src.po1) y) x
ORDER BY heard_at, packet_hash, observer_id;
DROP TABLE planguard_src.po1;

UPDATE planguard_src.pk p SET observation_count = c.n, last_heard_at = greatest(p.last_heard_at, c.mx)
FROM (SELECT packet_hash, count(*) n, max(heard_at) mx FROM planguard_src.po GROUP BY 1) c
WHERE c.packet_hash = p.packet_hash AND (p.observation_count <> c.n OR c.mx > p.last_heard_at);
