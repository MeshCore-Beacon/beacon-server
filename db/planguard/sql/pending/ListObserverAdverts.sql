-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: ListObserverAdverts :many
SELECT
  po.id,
  encode(po.packet_hash, 'hex') AS packet_hash_hex,
  p.payload_type,
  po.iata,
  po.heard_at,
  po.rssi,
  po.snr,
  po.hop_count,
  n.name AS node_name,
  COALESCE(encode(p.origin_pubkey, 'hex'), '')::text AS node_public_key
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
LEFT JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE po.observer_id = $1
  AND po.payload_type = 4
  AND po.id > $2::bigint
ORDER BY po.id ASC
LIMIT $3
