-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: ListPacketsByScopes :many
WITH page AS MATERIALIZED (
  SELECT x.packet_hash, x.last_heard_at AS k
  FROM transport_scopes s
  CROSS JOIN LATERAL (
    SELECT p.packet_hash, p.last_heard_at FROM packets p
    WHERE p.scope_id = s.id
      AND (p.last_heard_at, p.packet_hash) < ($1::timestamptz, $2::bytea)
    ORDER BY p.last_heard_at DESC, p.packet_hash DESC
    LIMIT $3) x
  WHERE s.name = ANY($4::text[])
  ORDER BY x.last_heard_at DESC, x.packet_hash DESC
  LIMIT $3)
SELECT p.packet_hash, p.payload_type,
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
       p.first_heard_at, p.last_heard_at, p.scope_id, ts.name AS scope_name,
       p.observation_count AS observation_count,
       po.observer_id AS latest_observer_id, o.display_name AS latest_observer_name,
       COALESCE(po.iata, ''::bpchar) AS latest_observer_iata,
       COALESCE(po.path_length_byte, 0::smallint) AS latest_observer_path_length_byte,
       COALESCE(po.hash_size, 0::smallint) AS latest_observer_hash_size,
       COALESCE(po.hop_count, 0::smallint) AS latest_observer_hop_count,
       po.path_bytes AS latest_observer_path_bytes, p.raw_payload, p.origin_pubkey
FROM page pg
JOIN packets p ON p.packet_hash = pg.packet_hash
LEFT JOIN LATERAL (
  SELECT observer_id, iata, path_length_byte, hash_size, hop_count, path_bytes
  FROM packet_observations WHERE packet_hash = p.packet_hash
  ORDER BY heard_at DESC, id DESC
  LIMIT 1) po ON true
LEFT JOIN observers o ON o.id = po.observer_id
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY pg.k DESC, pg.packet_hash DESC
