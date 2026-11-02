-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Fixture step 10: sampling helpers and the reference tables (regions, scopes, channels, observers, nodes and the
-- regions each node is heard in). Every identifier is synthetic: ids and keys are digests of a rank, names are
-- scope_<n>, channel_<n>, observer_<n> and node_<n>. The fixture's clock stops at 2026-10-18 19:00 UTC (routes)
-- and 2026-10-18 17:00 UTC (packets), as the copy the distributions were read from.
SELECT setseed(0.42);

-- mkcum(src, value, weight, name) builds planguard_src.<name>(v, lo): value with the cumulative weight below it, as a
-- fraction. A step draws from it with
--   (SELECT array_agg(v ORDER BY lo) FROM planguard_src.<name>)[width_bucket(r, (SELECT array_agg(lo ORDER BY lo) FROM planguard_src.<name>))]
-- for a uniform r in [0, 1): both arrays are computed once per statement and width_bucket searches them.
CREATE FUNCTION planguard_src.mkcum(src text, valcol text, wcol text, tgt text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  EXECUTE format('CREATE TABLE planguard_src.%I AS SELECT %s AS v, '
    'coalesce(sum(%s) OVER (ORDER BY %s ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)::float8 / sum(%s) OVER () AS lo '
    'FROM %s WHERE %s > 0', tgt, valcol, wcol, valcol, wcol, src, wcol);
END $$;

-- ---------- regions ----------
-- Every code the distributions name, plus NEW (the stale region, loaded after ANALYZE in step 90) and WRE (a
-- region that never has routes).
INSERT INTO iata_codes (iata, display_name, added_at)
SELECT iata, 'region ' || iata, timestamptz '2026-01-01 00:00+00'
FROM (SELECT iata FROM planguard_src.routes_regions UNION SELECT iata FROM planguard_src.routes_iata_hop
      UNION SELECT iata FROM planguard_src.po_iata UNION SELECT iata FROM planguard_src.node_iatas_iata
      UNION SELECT home_iata FROM planguard_src.po_observer
      UNION SELECT home_iata FROM planguard_src.cm_channel WHERE home_iata IS NOT NULL
      UNION SELECT 'NEW' UNION SELECT 'WRE') s
ORDER BY iata;

-- ---------- transport scopes: one per rank of packets_scope ----------
INSERT INTO transport_scopes (id, name, display_name, transport_key, key_fingerprint, created_at)
SELECT k, 'scope_' || k, 'Scope ' || k, decode(md5('scope-key' || k), 'hex'), substr(decode(md5('scope-fp' || k), 'hex'), 1, 8),
       timestamptz '2026-01-01 00:00+00'
FROM generate_series(1, (SELECT greatest(count(*), 1491) FROM planguard_src.packets_scope)) k;

-- ---------- channels: one per rank of cm_channel, then hash-only channels (a third keyed) up to 729 ----------
CREATE TABLE planguard_src.channel_rank AS
SELECT rank, keyed, row_number() OVER (PARTITION BY keyed ORDER BY rank) - 1 AS nth
FROM (SELECT k AS rank, (k <= (SELECT count(*) FROM planguard_src.cm_channel) OR k % 3 = 0) AS keyed FROM generate_series(1, 729) k) s;
-- Staged in planguard_src.ch; step 60 inserts them with their message counts. A keyless channel is known by its
-- one-byte hash alone, so those hashes are distinct.
CREATE TABLE planguard_src.ch AS
SELECT r.rank AS id, CASE WHEN r.keyed THEN substr(decode(md5('channel-hash' || r.rank), 'hex'), 1, 1)
                          ELSE set_byte('\x00'::bytea, 0, r.nth::int) END AS channel_hash,
       CASE WHEN r.keyed THEN substr(decode(md5('channel-fp' || r.rank), 'hex'), 1, 8) END AS key_fingerprint,
       CASE WHEN r.keyed THEN 'channel_' || r.rank END AS name,
       CASE WHEN r.keyed AND r.rank % 2 = 0 THEN '#channel_' || r.rank END AS hashtag,
       r.keyed AND r.rank % 2 = 0 AS is_hashtag, r.rank = 1 AS is_public, r.keyed AS key_known,
       timestamptz '2026-09-01 00:00+00' + (r.rank % 40) * interval '1 day' AS first_seen,
       timestamptz '2026-10-18 17:00+00' - (r.rank * r.rank % 9973) * interval '1 minute' - (random() * 59) * interval '1 s' AS last_seen
FROM planguard_src.channel_rank r;

-- ---------- observers: one per rank of po_observer, at its home site ----------
INSERT INTO observers (id, public_key, display_name, observer_type, first_seen, last_seen, observation_count, last_iata, last_iata_at)
SELECT md5('observer' || o.rank)::uuid, decode(md5('observer-key' || o.rank) || md5('observer-key2' || o.rank), 'hex'),
       'observer_' || o.rank, CASE WHEN o.rank % 7 = 0 THEN 'room' ELSE 'repeater' END,
       timestamptz '2026-09-01 00:00+00', timestamptz '2026-10-18 17:00+00' - (o.rank % 600) * interval '1 minute',
       o.n, o.home_iata, timestamptz '2026-10-18 16:00+00'
FROM planguard_src.po_observer o;

-- ---------- nodes: 1/20 of the copy's, by type ----------
CREATE TABLE planguard_src.node_rank AS
SELECT row_number() OVER (ORDER BY t.node_type, g) AS rank, t.node_type,
       g <= round(t.n_drift60 / 20.0) AS drift60, g <= round(t.n_drift / 20.0) AS has_drift
FROM planguard_src.nodes_types t CROSS JOIN LATERAL generate_series(1, greatest(1, round(t.n / 20.0)::int)) g;
-- Relays whose keys start with the hash prefixes the route-search cases look up: two per prefix, each heard at its
-- home region, and the prefixes searched as a pair share a region (1410 + 3d67 in RTM, 5731 + 0355 in PRG,
-- f877 + 0867 in DEN).
CREATE TABLE planguard_src.prefix_relay AS
SELECT hx, home, row_number() OVER (ORDER BY hx, home) AS k
FROM (VALUES ('1410', 'RTM'), ('1410', 'AMS'), ('3d67', 'RTM'), ('3d67', 'EIN'), ('5731', 'PRG'), ('5731', 'SEA'),
             ('0355', 'PRG'), ('0355', 'BWE'), ('f877', 'DEN'), ('f877', 'SYD'), ('0867', 'DEN'), ('0867', 'MAD'),
             ('fade', 'SEA'), ('fade', 'PDX'), ('2525', 'SEA'), ('2525', 'BER')) p(hx, home);
INSERT INTO nodes (id, public_key, node_type, name, last_advert_at, first_seen, last_seen, device_clock_drift_seconds)
SELECT md5('node' || n.rank)::uuid,
       CASE WHEN pr.hx IS NOT NULL THEN decode(pr.hx || substr(md5('node-key' || n.rank) || md5('node-key2' || n.rank), 5), 'hex')
            ELSE decode(md5('node-key' || n.rank) || md5('node-key2' || n.rank), 'hex') END,
       n.node_type, 'node_' || n.rank,
       timestamptz '2026-10-18 17:00+00' - (random() * 72) * interval '1 hour',
       timestamptz '2026-09-01 00:00+00' + (random() * 40) * interval '1 day',
       timestamptz '2026-10-18 19:00+00' - (random() * random() * 30) * interval '1 day',
       CASE WHEN n.drift60 THEN (61 + floor(random() * 3600)::int) * (CASE WHEN random() < 0.5 THEN -1 ELSE 1 END)
            WHEN n.has_drift THEN floor(random() * 121)::int - 60 END
FROM planguard_src.node_rank n
LEFT JOIN planguard_src.prefix_relay pr
  ON n.node_type = 2 AND pr.k = n.rank - (SELECT min(rank) FROM planguard_src.node_rank WHERE node_type = 2) + 1;

-- node_iatas: 1/20 of the copy's nodes per region, at least 8 nodes in every region that has routes; a node is
-- heard in about five regions. The planted prefix relays are heard at their home region.
CREATE TABLE planguard_src.region_nodes AS
SELECT i.iata, greatest(round(coalesce(ni.n, 0) / 20.0)::int, CASE WHEN rr.iata IS NOT NULL THEN 8 ELSE 1 END) AS n
FROM iata_codes i
LEFT JOIN planguard_src.node_iatas_iata ni ON ni.iata = i.iata
LEFT JOIN (SELECT iata FROM planguard_src.routes_regions UNION SELECT iata FROM planguard_src.routes_iata_hop UNION SELECT 'NEW') rr ON rr.iata = i.iata;
INSERT INTO node_iatas (node_id, iata, first_heard, last_heard, observation_count)
SELECT DISTINCT ON (s.id, s.iata) s.id, s.iata, timestamptz '2026-09-01 00:00+00' + (random() * 40) * interval '1 day',
       timestamptz '2026-10-18 17:00+00' - (random() * random() * 14) * interval '1 day', 1 + floor(random() * 500)::int
FROM (
  SELECT x.iata, x.id FROM planguard_src.region_nodes r
  CROSS JOIN LATERAL (SELECT r.iata, n.id FROM nodes n ORDER BY md5(r.iata || n.id::text) LIMIT r.n) x
  UNION ALL
  SELECT pr.home, md5('node' || (pr.k + (SELECT min(rank) FROM planguard_src.node_rank WHERE node_type = 2) - 1))::uuid
  FROM planguard_src.prefix_relay pr
) s ORDER BY s.id, s.iata;
INSERT INTO node_short_ids (node_id, iata, prefix_4)
SELECT ni.node_id, ni.iata, substr(n.public_key, 1, 4) FROM node_iatas ni JOIN nodes n ON n.id = ni.node_id;
