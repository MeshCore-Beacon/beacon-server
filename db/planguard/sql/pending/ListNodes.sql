-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: ListNodes :many
WITH page AS (
SELECT n.id, n.public_key, n.node_type, n.name, n.latitude, n.longitude, n.last_seen,
  n.radio_freq_mhz, n.radio_sf, n.radio_bw_khz, ts.name AS default_scope_name
FROM nodes n
LEFT JOIN transport_scopes ts ON ts.id = n.default_scope_id
WHERE
  ($1 = 0 OR n.node_type = $1)
  AND (COALESCE(cardinality($2::bpchar[]), 0) = 0 OR n.id IN (SELECT node_id FROM node_iatas WHERE iata = ANY($2::bpchar[])))
  AND (
    $3::text = 'any'
    OR ($3::text = 'true' AND n.supports_multibyte_paths = TRUE)
    OR ($3::text = 'false' AND n.supports_multibyte_paths = FALSE)
  )
  AND (
    $4::text = 'any'
    OR ($4::text = 'true' AND n.supports_multibyte_traces = TRUE)
    OR ($4::text = 'false' AND n.supports_multibyte_traces = FALSE)
  )
  AND ($5::bytea IS NULL OR n.public_key = $5)
  AND ($6 = '' OR n.name ILIKE '%' || $6 || '%')
  AND (n.last_seen, n.id) < ($7::timestamptz, $12::uuid)
  AND ($9::text = '' OR ts.name = $9::text)
  AND ($11::text = '' OR encode(n.public_key, 'hex') ILIKE $11 || '%')
ORDER BY n.last_seen DESC, n.id DESC
LIMIT $8
)
SELECT n.id, n.public_key, n.node_type, n.name, n.latitude, n.longitude, n.last_seen,
  n.radio_freq_mhz, n.radio_sf, n.radio_bw_khz, n.default_scope_name,
  (SELECT json_agg(json_build_object('iata', ni.iata, 'lastHeard', (extract(epoch from ni.last_heard) * 1000)::bigint) ORDER BY ni.last_heard DESC)
   FROM node_iatas ni WHERE ni.node_id = n.id) AS iatas,
  EXISTS (SELECT 1 FROM observers o WHERE o.public_key = n.public_key) AS is_observer,
  (SELECT o.id FROM observers o WHERE o.public_key = n.public_key LIMIT 1) AS observer_id,
  (SELECT COUNT(DISTINCT nn.neighbor_id) FROM node_neighbors nn WHERE nn.node_id = n.id)::bigint AS known_neighbor_count,
  (CASE WHEN $10::bool THEN
    (SELECT COALESCE(array_agg(DISTINCT nn.neighbor_id), '{}'::uuid[]) FROM node_neighbors nn WHERE nn.node_id = n.id)
  ELSE NULL END)::uuid[] AS neighbor_ids
FROM page n
ORDER BY n.last_seen DESC, n.id DESC
