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
}
