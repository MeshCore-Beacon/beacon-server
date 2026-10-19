-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: AddChannelMessageIATA :exec
-- A stored observation's site for a message another statement created. A no-op when the packet has no message.
INSERT INTO channel_message_iatas (message_id, iata, channel_id, scope_id)
SELECT cm.id, @iata::bpchar, cm.channel_id, p.scope_id
FROM channel_messages cm
JOIN packets p ON p.packet_hash = cm.packet_hash
WHERE cm.packet_hash = @packet_hash::bytea
ON CONFLICT (message_id, iata) DO NOTHING;
