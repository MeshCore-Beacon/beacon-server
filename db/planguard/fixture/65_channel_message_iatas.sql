-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Fixture step 65: channel_message_iatas, as the backfill fills it: one row per message and site that heard it.
INSERT INTO channel_message_iatas (message_id, iata, channel_id, scope_id)
SELECT DISTINCT cm.id, po.iata, cm.channel_id, p.scope_id
FROM channel_messages cm
JOIN packets p ON p.packet_hash = cm.packet_hash
JOIN packet_observations po ON po.packet_hash = cm.packet_hash
ORDER BY cm.id, po.iata;
