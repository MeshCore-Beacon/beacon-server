-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: GetTopNodesProbe
WITH src AS (
  SELECT origin_pubkey, observations, last_heard, iata, name, node_type
  FROM analytics_hourly_advert_hearings
  WHERE hour >= $1::timestamptz AND COALESCE(cardinality($2::bpchar[]), 0) = 0
  UNION ALL
  SELECT a.origin_pubkey, a.observations, a.last_heard, a.iata, a.name, a.node_type
  FROM generate_series(GREATEST($1::timestamptz, (SELECT min(hour) FROM analytics_hourly_advert_hearings)), date_trunc('hour', now(), 'UTC'), interval '1 hour') g(h)
  CROSS JOIN (SELECT DISTINCT unnest($2::bpchar[]) AS iata) i
  CROSS JOIN LATERAL (SELECT x.origin_pubkey, x.observations, x.last_heard, x.iata, x.name, x.node_type
                      FROM analytics_hourly_advert_hearings x WHERE x.hour = g.h AND x.iata = i.iata OFFSET 0) a
  WHERE cardinality($2::bpchar[]) > 0
), ranked AS (
  SELECT origin_pubkey, SUM(observations)::bigint AS observation_count, MAX(last_heard) AS last_heard,
         MAX(iata) AS iata, MAX(name) AS name, MAX(node_type) AS node_type
  FROM src
  GROUP BY origin_pubkey
  ORDER BY observation_count DESC, origin_pubkey
  LIMIT $3
)
SELECT n.id AS node_id, encode(r.origin_pubkey, 'hex') AS public_key,
       COALESCE(n.name, r.name, '')::text AS name, COALESCE(n.node_type, r.node_type, 0)::smallint AS node_type,
       r.iata::bpchar AS iata, r.observation_count, r.last_heard::timestamptz AS last_heard
FROM ranked r
LEFT JOIN nodes n ON n.public_key = r.origin_pubkey
ORDER BY r.observation_count DESC, r.origin_pubkey
