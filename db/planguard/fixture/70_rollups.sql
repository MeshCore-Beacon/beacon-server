-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Fixture step 70: hourly rollups for 381 complete hours (2026-10-02 19:00 to 2026-10-18 15:00 UTC). Each rollup
-- hour holds the aggregates of the fixture hour at the same offset in the 72-hour packet window, so every hour has
-- a day's shape. observer_identity keeps the six busiest observers per (hour, region), as a real copy holds about
-- that many. Each aggregate is taken once per fixture hour, then copied to the rollup hours it stands for.
INSERT INTO analytics_rollup_hours (hour, status, rolled_at)
SELECT h, 'complete', h + interval '1 hour 5 min'
FROM generate_series(timestamptz '2026-10-02 19:00+00', timestamptz '2026-10-18 15:00+00', interval '1 hour') h;
CREATE TABLE planguard_src.hmap AS
SELECT hour, timestamptz '2026-10-15 17:00+00'
             + ((extract(epoch FROM hour - timestamptz '2026-10-02 19:00+00') / 3600)::int % 72) * interval '1 hour' AS src
FROM analytics_rollup_hours;
CREATE TABLE planguard_src.po_src AS
SELECT date_trunc('hour', heard_at) AS src, iata, observer_id, payload_type, hop_count, snr, rssi, heard_at, packet_hash
FROM packet_observations;
CREATE TABLE planguard_src.pkh AS
SELECT p.packet_hash, date_trunc('hour', p.first_heard_at) AS src, p.scope_id, p.payload_type, p.route_type, p.origin_pubkey, s.iatas
FROM packets p
JOIN (SELECT packet_hash, array_agg(DISTINCT iata ORDER BY iata) AS iatas FROM packet_observations GROUP BY 1) s ON s.packet_hash = p.packet_hash;

INSERT INTO analytics_hourly_iata_observations (hour, iata, observation_count)
SELECT m.hour, a.iata, a.n
FROM (SELECT src, iata, count(*) n FROM planguard_src.po_src GROUP BY 1, 2) a JOIN planguard_src.hmap m ON m.src = a.src
ORDER BY 1, 2;
INSERT INTO analytics_hourly_packet_sets (hour, iatas, packets)
SELECT m.hour, a.iatas, a.n
FROM (SELECT src, iatas, count(*) n FROM planguard_src.pkh GROUP BY 1, 2) a JOIN planguard_src.hmap m ON m.src = a.src
ORDER BY 1, 2;
INSERT INTO analytics_hourly_scope_sets (hour, iatas, scope_id, packets)
SELECT m.hour, a.iatas, a.scope_id, a.n
FROM (SELECT src, iatas, scope_id, count(*) n FROM planguard_src.pkh WHERE scope_id IS NOT NULL GROUP BY 1, 2, 3) a
JOIN planguard_src.hmap m ON m.src = a.src
ORDER BY 1, 2, 3;
INSERT INTO analytics_hourly_observer_identity (hour, iata, observer_id, observation_count)
SELECT m.hour, a.iata, a.observer_id, a.n
FROM (SELECT src, iata, observer_id, n FROM (
        SELECT src, iata, observer_id, count(*) n, row_number() OVER (PARTITION BY src, iata ORDER BY count(*) DESC, observer_id) rk
        FROM planguard_src.po_src GROUP BY 1, 2, 3) r WHERE rk <= 6) a
JOIN planguard_src.hmap m ON m.src = a.src
ORDER BY 1, 2, 3;
INSERT INTO analytics_hourly_paths (hour, iata, category, hash_bytes, entries, receptions)
SELECT m.hour, a.iata, c.category, 1, a.entries, a.n
FROM (SELECT src, iata, least(hop_count, 8) AS entries, count(*) n FROM planguard_src.po_src GROUP BY 1, 2, 3) a
JOIN planguard_src.hmap m ON m.src = a.src CROSS JOIN (VALUES (0), (1)) c(category)
ORDER BY 1, 2, 3, 5;
INSERT INTO analytics_hourly_signal (hour, iata, kind, snr_bin, rssi_bin, receptions, snr_samples, snr_sum, rssi_samples, rssi_sum)
SELECT m.hour, a.iata, k.kind, a.snr_bin, a.rssi_bin, a.n, a.n, a.snr_sum, a.n, a.rssi_sum
FROM (SELECT src, iata, floor(snr / 10)::int AS snr_bin, floor(rssi / 30)::int AS rssi_bin, count(*) n, sum(snr) snr_sum, sum(rssi) rssi_sum
      FROM planguard_src.po_src GROUP BY 1, 2, 3, 4) a
JOIN planguard_src.hmap m ON m.src = a.src CROSS JOIN (VALUES (1), (3)) k(kind)
ORDER BY 1, 2, 3, 4, 5;
-- Adverts: hearings per (hour, region, origin) and distinct advert packets per (hour, origin, region set).
INSERT INTO analytics_hourly_advert_hearings (hour, iata, origin_pubkey, observations, last_heard, name, node_type)
SELECT m.hour, a.iata, a.origin_pubkey, a.n, a.last_heard - a.src + m.hour, n.name, n.node_type
FROM (SELECT po.src, po.iata, p.origin_pubkey, count(*) n, max(po.heard_at) last_heard
      FROM planguard_src.po_src po JOIN packets p ON p.packet_hash = po.packet_hash
      WHERE p.payload_type = 4 AND p.origin_pubkey IS NOT NULL GROUP BY 1, 2, 3) a
JOIN planguard_src.hmap m ON m.src = a.src
LEFT JOIN nodes n ON n.public_key = a.origin_pubkey
ORDER BY 1, 2, 3;
INSERT INTO analytics_hourly_advert_sets (hour, origin_pubkey, iatas, advert_packets, flood_packets, direct_packets)
SELECT m.hour, a.origin_pubkey, a.iatas, a.n, a.flood, a.direct
FROM (SELECT src, origin_pubkey, iatas, count(*) n, count(*) FILTER (WHERE route_type IN (0, 1)) flood,
             count(*) FILTER (WHERE route_type IN (2, 3)) direct
      FROM planguard_src.pkh WHERE payload_type = 4 AND origin_pubkey IS NOT NULL GROUP BY 1, 2, 3) a
JOIN planguard_src.hmap m ON m.src = a.src
ORDER BY 1, 2, 3;
DROP TABLE planguard_src.po_src, planguard_src.pkh;
