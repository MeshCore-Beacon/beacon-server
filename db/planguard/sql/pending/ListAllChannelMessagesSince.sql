-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: ListAllChannelMessagesSince :many
SELECT cm.id, cm.channel_id, cm.packet_hash, cm.sender_name, cm.sender_pubkey, cm.content, cm.sent_at, encode(cm.packet_hash, 'hex') AS packet_hash_hex, c.channel_hash, ts.name AS scope_name,
       p.transport_codes_present, p.observation_count AS observation_count
FROM channel_messages cm
JOIN channels c ON c.id = cm.channel_id
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE cm.id < $3::bigint
  AND cm.sent_at >= $1::timestamptz
  AND ($2::int IS NULL OR p.scope_id = $2::int)
  AND EXISTS (SELECT 1 FROM packet_observations po WHERE po.packet_hash = cm.packet_hash)
ORDER BY cm.id DESC
LIMIT $4
