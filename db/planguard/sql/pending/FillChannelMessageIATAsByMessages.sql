-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: FillChannelMessageIATAsByMessages :execrows
INSERT INTO channel_message_iatas (message_id, iata, channel_id, scope_id)
SELECT DISTINCT cm.id, po.iata, cm.channel_id, p.scope_id
FROM channel_messages cm
JOIN packets p USING (packet_hash)
JOIN packet_observations po USING (packet_hash)
WHERE cm.id > $1::bigint AND cm.id <= $2::bigint
ON CONFLICT DO NOTHING
