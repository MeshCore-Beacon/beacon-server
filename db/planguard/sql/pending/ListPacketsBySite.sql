-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: ListPacketsBySite :many
WITH page AS MATERIALIZED (
  SELECT ps.packet_hash, ps.site_heard_at AS k
  FROM packet_sites ps
  WHERE ps.iata = $1::bpchar
    AND (ps.site_heard_at, ps.packet_hash) < ($2::timestamptz, $3::bytea)
  ORDER BY ps.site_heard_at DESC, ps.packet_hash DESC
  LIMIT $4)
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
       pg.k AS site_heard_at,
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
