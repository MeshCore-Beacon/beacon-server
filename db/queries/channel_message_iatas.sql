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

-- name: FillChannelMessageIATAsByMessages :execrows
-- Every site of every message in (after_id, through_id]: the backfill and the reconcile's message part.
INSERT INTO channel_message_iatas (message_id, iata, channel_id, scope_id)
SELECT DISTINCT cm.id, po.iata, cm.channel_id, p.scope_id
FROM channel_messages cm
JOIN packets p ON p.packet_hash = cm.packet_hash
JOIN packet_observations po ON po.packet_hash = cm.packet_hash
WHERE cm.id > @after_id::bigint AND cm.id <= @through_id::bigint
ON CONFLICT (message_id, iata) DO NOTHING;

-- name: FillChannelMessageIATAsByObservations :execrows
-- The site of every group-text observation in (after_id, through_id] whose packet has a message.
-- An untyped observation is checked too: older rows may have no payload type.
INSERT INTO channel_message_iatas (message_id, iata, channel_id, scope_id)
SELECT DISTINCT cm.id, po.iata, cm.channel_id, p.scope_id
FROM packet_observations po
JOIN channel_messages cm ON cm.packet_hash = po.packet_hash
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.id > @after_id::bigint AND po.id <= @through_id::bigint AND COALESCE(po.payload_type, 5) = 5
ON CONFLICT (message_id, iata) DO NOTHING;

-- name: MaxObservationID :one
SELECT COALESCE(max(id), 0)::bigint AS id FROM packet_observations;

-- name: MaxChannelMessageID :one
SELECT COALESCE(max(id), 0)::bigint AS id FROM channel_messages;

-- name: NextChannelMessageID :one
-- The first message id above after_id, or 0 when there is none.
SELECT COALESCE(min(id), 0)::bigint AS id FROM channel_messages WHERE id > @after_id::bigint;

-- name: CheckChannelMessageIATAs :one
-- Exact check of messages in (after_id, through_id]: pairs the definition has and the table lacks (counting
-- observations up to obs_through only), and table pairs no stored observation backs.
WITH def AS (
  SELECT DISTINCT cm.id AS message_id, po.iata, cm.channel_id, p.scope_id, po.id <= @obs_through::bigint AS settled
  FROM channel_messages cm
  JOIN packets p ON p.packet_hash = cm.packet_hash
  JOIN packet_observations po ON po.packet_hash = cm.packet_hash
  WHERE cm.id > @after_id::bigint AND cm.id <= @through_id::bigint
), have AS (
  SELECT i.message_id, i.iata, i.channel_id, i.scope_id
  FROM channel_message_iatas i
  WHERE i.message_id > @after_id::bigint AND i.message_id <= @through_id::bigint
)
SELECT
  (SELECT count(*) FROM (SELECT message_id, iata, channel_id, scope_id FROM def WHERE settled
                         EXCEPT SELECT message_id, iata, channel_id, scope_id FROM have) m)::bigint AS missing,
  (SELECT count(*) FROM (SELECT message_id, iata, channel_id, scope_id FROM have
                         EXCEPT SELECT message_id, iata, channel_id, scope_id FROM def) x)::bigint AS extra;
