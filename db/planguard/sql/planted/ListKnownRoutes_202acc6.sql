-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: ListKnownRoutes :many
SELECT r.id, r.node_ids, r.hash_prefix, r.iata, r.hop_count, r.first_seen, r.last_seen, r.observation_count
FROM ((
SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
FROM known_routes
WHERE $1 = ''
  AND ($2 = 0 OR hop_count = $2)
  AND ($3::timestamptz IS NULL
       OR date_trunc('milliseconds', last_seen, 'UTC') < $3
       OR ($5::bigint > 0 AND date_trunc('milliseconds', last_seen, 'UTC') = $3 AND id < $5))
ORDER BY date_trunc('milliseconds', last_seen, 'UTC') DESC, id DESC
LIMIT $4
)
UNION ALL
(
SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
FROM known_routes
WHERE $1 <> '' AND iata::text = $1
  AND iata >= $1::bpchar AND iata <= $1::bpchar
  AND ($2 = 0 OR hop_count = $2)
  AND ($3::timestamptz IS NULL
       OR date_trunc('milliseconds', last_seen, 'UTC') < $3
       OR ($5::bigint > 0 AND date_trunc('milliseconds', last_seen, 'UTC') = $3 AND id < $5))
ORDER BY iata, date_trunc('milliseconds', last_seen, 'UTC') DESC, id DESC
LIMIT $4
)) r
ORDER BY date_trunc('milliseconds', r.last_seen, 'UTC') DESC, r.id DESC
LIMIT $4
