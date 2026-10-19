// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import "strings"

// legacyListPackets is the pre-change global packets statement, verbatim. Args: $1 payload_types, $2 route_types,
// $3 since, $4 until, $5 cursor (all timestamptz, NULL when absent), $6 limit, $7 scopes.
const legacyListPackets = `-- name: ListPackets :many
SELECT
  p.packet_hash,
  p.payload_type,
  COALESCE(CASE
    WHEN p.payload_type = 4 AND jsonb_typeof(p.parsed_payload #> '{appData,name}') = 'string'
      THEN p.parsed_payload #>> '{appData,name}'
    WHEN p.payload_type = 3 AND p.parsed_payload ->> 'type' = 'ACK'
      AND jsonb_typeof(p.parsed_payload -> 'checksum') = 'string'
      AND p.parsed_payload ->> 'checksum' ~ '^[0-9a-fA-F]{8}$'
      THEN 'ACK ' || lower(p.parsed_payload ->> 'checksum')
    WHEN p.payload_type = 9 AND p.parsed_payload ->> 'type' IN ('TRACE', 'PING')
      AND jsonb_typeof(p.parsed_payload -> 'traceTag') = 'string'
      AND p.parsed_payload ->> 'traceTag' ~ '^[0-9a-fA-F]{8}$'
      THEN (p.parsed_payload ->> 'type') || ' ' || lower(p.parsed_payload ->> 'traceTag')
    END, '')::text AS summary,
  p.route_type,
  p.first_heard_at,
  p.last_heard_at,
  p.scope_id,
  ts.name AS scope_name,
  p.observation_count AS observation_count,
  -- sqlc loses LATERAL nullability; these scalar defaults are ignored when observer_id is NULL.
  po.observer_id AS latest_observer_id,
  o.display_name AS latest_observer_name,
  COALESCE(po.iata, ''::bpchar) AS latest_observer_iata,
  COALESCE(po.path_length_byte, 0::smallint) AS latest_observer_path_length_byte,
  COALESCE(po.hash_size, 0::smallint) AS latest_observer_hash_size,
  COALESCE(po.hop_count, 0::smallint) AS latest_observer_hop_count,
  po.path_bytes AS latest_observer_path_bytes,
  p.raw_payload,
  p.origin_pubkey
FROM packets p
LEFT JOIN LATERAL (
  SELECT observer_id, iata, path_length_byte, hash_size, hop_count, path_bytes
  FROM packet_observations
  WHERE packet_hash = p.packet_hash
  ORDER BY heard_at DESC
  LIMIT 1
) po ON true
LEFT JOIN observers o ON o.id = po.observer_id
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE
  (COALESCE(cardinality($1::smallint[]), 0) = 0 OR p.payload_type = ANY($1::smallint[]))
  AND (COALESCE(cardinality($2::smallint[]), 0) = 0 OR p.route_type = ANY($2::smallint[]))
  AND ($3::timestamptz IS NULL OR p.first_heard_at >= $3)
  AND ($4::timestamptz IS NULL OR p.first_heard_at <= $4)
  AND ($5::timestamptz IS NULL OR p.last_heard_at < $5)
  AND (COALESCE(cardinality($7::text[]), 0) = 0 OR ts.name = ANY($7::text[]))
ORDER BY p.last_heard_at DESC
LIMIT $6
`

// legacyListPacketsDet is today's statement plus the two tiebreaks only.
var legacyListPacketsDet = strings.NewReplacer(
	"ORDER BY p.last_heard_at DESC\nLIMIT $6", "ORDER BY p.last_heard_at DESC, p.packet_hash DESC\nLIMIT $6",
	"ORDER BY heard_at DESC\n  LIMIT 1", "ORDER BY heard_at DESC, id DESC\n  LIMIT 1",
).Replace(legacyListPackets)

func init() {
	if !strings.Contains(legacyListPacketsDet, "p.last_heard_at DESC, p.packet_hash DESC\nLIMIT $6") ||
		!strings.Contains(legacyListPacketsDet, "heard_at DESC, id DESC\n  LIMIT 1") {
		panic("legacyListPacketsDet: a tiebreak replacement did not apply")
	}
	if strings.Count(legacyListNodesDet, "n.last_seen DESC, n.id DESC\n") != 2 {
		panic("legacyListNodesDet: a tiebreak replacement did not apply")
	}
	if !strings.Contains(legacyListObserversDet, "o.last_seen DESC, o.id DESC\nLIMIT $7") {
		panic("legacyListObserversDet: the tiebreak replacement did not apply")
	}
}

// legacyListNodes is the pre-change ListNodes statement, verbatim. Args: $1 node_type, $2 iatas, $3 paths tristate,
// $4 traces tristate, $5 pubkey, $6 name, $7 cursor (NULL when absent), $8 limit, $9 scope, $10 neighbors,
// $11 pubkey prefix.
const legacyListNodes = `-- name: ListNodes :many
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
  AND ($7::timestamptz IS NULL OR n.last_seen < $7)
  AND ($9::text = '' OR ts.name = $9::text)
  AND ($11::text = '' OR encode(n.public_key, 'hex') ILIKE $11 || '%')
ORDER BY n.last_seen DESC
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
ORDER BY n.last_seen DESC
`

// legacyListNodesDet is today's ListNodes plus the id tiebreak in both ORDER BYs.
var legacyListNodesDet = strings.NewReplacer(
	"ORDER BY n.last_seen DESC\nLIMIT $8", "ORDER BY n.last_seen DESC, n.id DESC\nLIMIT $8",
	"FROM page n\nORDER BY n.last_seen DESC\n", "FROM page n\nORDER BY n.last_seen DESC, n.id DESC\n",
).Replace(legacyListNodes)

// legacyListObservers is the pre-change ListObservers statement, verbatim. Args: $1 iatas, $2 type, $3 broker,
// $4 status, $5 name, $6 cursor (NULL when absent), $7 limit, $8 scope.
const legacyListObservers = `-- name: ListObservers :many
SELECT
  o.id,
  o.display_name,
  o.observer_type,
  o.last_status_at,
  o.radio_freq_mhz,
  o.radio_sf,
  o.radio_bw_khz,
  array_remove(array_agg(DISTINCT ts.name ORDER BY ts.name), NULL)::text[] AS scopes,
COALESCE(CASE
    WHEN GREATEST(COALESCE(o.last_status_at, o.last_seen), o.last_seen) > NOW() - INTERVAL '5 minutes' THEN 'online'
    ELSE 'offline'
END, 'offline')::text AS status,
COALESCE(o.last_iata, '')::text AS iata
FROM observers o
LEFT JOIN observer_brokers ob ON ob.observer_id = o.id
LEFT JOIN observer_scopes os ON os.observer_id = o.id
LEFT JOIN transport_scopes ts ON ts.id = os.scope_id
WHERE
  (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR o.last_iata = ANY($1::bpchar[]))
  AND ($2 = '' OR o.observer_type = $2)
  AND ($3 = '' OR ob.broker_name = $3)
  AND ($4 = '' OR CASE
    WHEN GREATEST(COALESCE(o.last_status_at, o.last_seen), o.last_seen) > NOW() - INTERVAL '5 minutes' THEN 'online'
    ELSE 'offline'
  END = $4)
  AND ($5 = '' OR o.display_name ILIKE '%' || $5 || '%')
  AND ($6::timestamptz IS NULL OR o.last_seen < $6)
  AND ($8::text = '' OR EXISTS (
    SELECT 1 FROM observer_scopes os2
    JOIN transport_scopes ts2 ON ts2.id = os2.scope_id
    WHERE os2.observer_id = o.id AND ts2.name = $8::text
  ))
GROUP BY o.id
ORDER BY o.last_seen DESC
LIMIT $7
`

// legacyListObserversDet is today's ListObservers plus the id tiebreak.
var legacyListObserversDet = strings.NewReplacer(
	"ORDER BY o.last_seen DESC\nLIMIT $7", "ORDER BY o.last_seen DESC, o.id DESC\nLIMIT $7",
).Replace(legacyListObservers)
