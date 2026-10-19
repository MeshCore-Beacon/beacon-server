-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: GetStatsTopAdvertisersProbe
WITH ranked AS (
  SELECT origin_pubkey, SUM(advert_packets)::bigint AS advert_count,
         SUM(flood_packets)::bigint AS flood_advert_count, SUM(direct_packets)::bigint AS direct_advert_count
  FROM analytics_hourly_advert_sets
  WHERE hour >= $2::timestamptz
    AND (COALESCE(cardinality($3::bpchar[]), 0) = 0 OR iatas && $3::bpchar[])
  GROUP BY origin_pubkey
  ORDER BY CASE WHEN $1::text = 'flood' THEN SUM(flood_packets) ELSE SUM(direct_packets) END DESC,
           advert_count DESC, origin_pubkey
  LIMIT $4
), heard_rows AS (
  SELECT h.origin_pubkey, h.last_heard, h.iata, h.name, h.node_type
  FROM analytics_hourly_advert_hearings h
  WHERE h.hour >= $2::timestamptz AND COALESCE(cardinality($3::bpchar[]), 0) = 0
    AND h.origin_pubkey IN (SELECT origin_pubkey FROM ranked)
  UNION ALL
  SELECT h.origin_pubkey, h.last_heard, h.iata, h.name, h.node_type
  FROM generate_series(GREATEST($2::timestamptz, (SELECT min(hour) FROM analytics_hourly_advert_hearings)), date_trunc('hour', now(), 'UTC'), interval '1 hour') g(h)
  CROSS JOIN (SELECT DISTINCT unnest($3::bpchar[]) AS iata) i
  CROSS JOIN LATERAL (SELECT x.origin_pubkey, x.last_heard, x.iata, x.name, x.node_type
                      FROM analytics_hourly_advert_hearings x WHERE x.hour = g.h AND x.iata = i.iata OFFSET 0) h
  WHERE cardinality($3::bpchar[]) > 0 AND h.origin_pubkey IN (SELECT origin_pubkey FROM ranked)
), heard AS (
  SELECT origin_pubkey, MAX(last_heard) AS last_heard, MAX(iata) AS iata, MAX(name) AS name, MAX(node_type) AS node_type
  FROM heard_rows GROUP BY origin_pubkey
)
SELECT n.id AS node_id, encode(r.origin_pubkey, 'hex') AS public_key,
       COALESCE(n.name, h.name, '')::text AS name, COALESCE(n.node_type, h.node_type, 0)::smallint AS node_type,
       r.advert_count, r.flood_advert_count, r.direct_advert_count,
       h.last_heard::timestamptz AS last_heard, COALESCE(h.iata, '')::bpchar AS iata
FROM ranked r
LEFT JOIN heard h ON h.origin_pubkey = r.origin_pubkey
LEFT JOIN nodes n ON n.public_key = r.origin_pubkey
ORDER BY CASE WHEN $1::text = 'flood' THEN r.flood_advert_count ELSE r.direct_advert_count END DESC,
         r.advert_count DESC, r.origin_pubkey
