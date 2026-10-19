-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: ListChannelMessagesPlainScoped :many
WITH hits AS MATERIALIZED (
  SELECT DISTINCT i.message_id
  FROM channel_message_iatas i
  WHERE i.channel_id = $1::int AND i.scope_id = $2::int AND i.message_id < $3::bigint
  ORDER BY i.message_id DESC
  LIMIT $4)
SELECT cm.id, cm.channel_id, cm.packet_hash, cm.sender_name, cm.sender_pubkey, cm.content, cm.sent_at, encode(cm.packet_hash, 'hex') AS packet_hash_hex, c.channel_hash, ts.name AS scope_name,
       p.transport_codes_present, p.observation_count AS observation_count
FROM hits h
JOIN channel_messages cm ON cm.id = h.message_id
JOIN channels c ON c.id = cm.channel_id
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY cm.id DESC
