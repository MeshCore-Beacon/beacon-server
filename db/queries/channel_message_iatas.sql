-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: AddChannelMessageIATA :one
-- A stored observation's site for a message another statement created. A no-op when the packet has no message;
-- found says whether the packet has a committed message (then the site is recorded, now or before).
WITH m AS (
  SELECT cm.id, cm.channel_id, p.scope_id
  FROM channel_messages cm
  JOIN packets p ON p.packet_hash = cm.packet_hash
  WHERE cm.packet_hash = @packet_hash::bytea
), added AS (
  INSERT INTO channel_message_iatas (message_id, iata, channel_id, scope_id)
  SELECT m.id, @iata::bpchar, m.channel_id, m.scope_id FROM m
  ON CONFLICT (message_id, iata) DO NOTHING
)
SELECT EXISTS (SELECT 1 FROM m) AS found;

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

-- name: ListChannelMessagesByIATAs :many
-- One channel filtered by sites: each site's newest message ids from the read model, merged.
WITH hits AS MATERIALIZED (
  SELECT m.message_id
  FROM unnest(@iatas::bpchar[]) AS req(iata)
  CROSS JOIN LATERAL (
    SELECT i.message_id FROM channel_message_iatas i
    WHERE i.channel_id = @channel_id::int AND i.iata = req.iata
      AND i.message_id < @cursor_id::bigint
    ORDER BY i.message_id DESC
    LIMIT @page_limit) m
  GROUP BY m.message_id
  ORDER BY m.message_id DESC
  LIMIT @page_limit)
SELECT cm.id, cm.channel_id, cm.packet_hash, cm.sender_name, cm.sender_pubkey, cm.content, cm.sent_at,
       encode(cm.packet_hash, 'hex') AS packet_hash_hex, c.channel_hash, ts.name AS scope_name,
       p.transport_codes_present, p.observation_count AS observation_count
FROM hits h
JOIN channel_messages cm ON cm.id = h.message_id
JOIN channels c ON c.id = cm.channel_id
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY cm.id DESC;

-- name: ListChannelMessagesByIATAsScoped :many
-- One channel filtered by sites and scope.
WITH hits AS MATERIALIZED (
  SELECT m.message_id
  FROM unnest(@iatas::bpchar[]) AS req(iata)
  CROSS JOIN LATERAL (
    SELECT i.message_id FROM channel_message_iatas i
    WHERE i.channel_id = @channel_id::int AND i.iata = req.iata AND i.scope_id = @scope_id::int
      AND i.message_id < @cursor_id::bigint
    ORDER BY i.message_id DESC
    LIMIT @page_limit) m
  GROUP BY m.message_id
  ORDER BY m.message_id DESC
  LIMIT @page_limit)
SELECT cm.id, cm.channel_id, cm.packet_hash, cm.sender_name, cm.sender_pubkey, cm.content, cm.sent_at,
       encode(cm.packet_hash, 'hex') AS packet_hash_hex, c.channel_hash, ts.name AS scope_name,
       p.transport_codes_present, p.observation_count AS observation_count
FROM hits h
JOIN channel_messages cm ON cm.id = h.message_id
JOIN channels c ON c.id = cm.channel_id
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY cm.id DESC;

-- name: ListChannelMessagesPlainScoped :many
-- One channel and scope: the read model has a row exactly when the message has a stored observation.
WITH hits AS MATERIALIZED (
  SELECT DISTINCT i.message_id
  FROM channel_message_iatas i
  WHERE i.channel_id = @channel_id::int AND i.scope_id = @scope_id::int AND i.message_id < @cursor_id::bigint
  ORDER BY i.message_id DESC
  LIMIT @page_limit)
SELECT cm.id, cm.channel_id, cm.packet_hash, cm.sender_name, cm.sender_pubkey, cm.content, cm.sent_at,
       encode(cm.packet_hash, 'hex') AS packet_hash_hex, c.channel_hash, ts.name AS scope_name,
       p.transport_codes_present, p.observation_count AS observation_count
FROM hits h
JOIN channel_messages cm ON cm.id = h.message_id
JOIN channels c ON c.id = cm.channel_id
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY cm.id DESC;

-- name: ListChannelMessagesPlain :many
-- One channel, unfiltered: a walk of the channel by id; a message counts once it has a stored observation.
SELECT cm.id, cm.channel_id, cm.packet_hash, cm.sender_name, cm.sender_pubkey, cm.content, cm.sent_at,
       encode(cm.packet_hash, 'hex') AS packet_hash_hex, c.channel_hash, ts.name AS scope_name,
       p.transport_codes_present, p.observation_count AS observation_count
FROM channel_messages cm
JOIN channels c ON c.id = cm.channel_id
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE cm.channel_id = @channel_id::int AND cm.id < @cursor_id::bigint
  AND EXISTS (SELECT 1 FROM packet_observations po WHERE po.packet_hash = cm.packet_hash)
ORDER BY cm.id DESC
LIMIT @page_limit;

-- name: ListChannelMessagesSinceWalk :many
-- One channel with since (no client sends it): a channel walk; sites filter through the read model.
SELECT cm.id, cm.channel_id, cm.packet_hash, cm.sender_name, cm.sender_pubkey, cm.content, cm.sent_at,
       encode(cm.packet_hash, 'hex') AS packet_hash_hex, c.channel_hash, ts.name AS scope_name,
       p.transport_codes_present, p.observation_count AS observation_count
FROM channel_messages cm
JOIN channels c ON c.id = cm.channel_id
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE cm.channel_id = @channel_id::int
  AND cm.sent_at >= @since::timestamptz
  AND EXISTS (SELECT 1 FROM packet_observations po WHERE po.packet_hash = cm.packet_hash)
  AND (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0
       OR EXISTS (SELECT 1 FROM channel_message_iatas i WHERE i.message_id = cm.id AND i.iata = ANY(@iatas::bpchar[])))
  AND (sqlc.narg('scope_id')::int IS NULL OR p.scope_id = sqlc.narg('scope_id')::int)
  AND cm.id < @cursor_id::bigint
ORDER BY cm.id DESC
LIMIT @page_limit;

-- name: ListAllChannelMessagesByIATAs :many
-- Every channel filtered by sites: each site's newest message ids from the read model, merged.
WITH hits AS MATERIALIZED (
  SELECT m.message_id
  FROM unnest(@iatas::bpchar[]) AS req(iata)
  CROSS JOIN LATERAL (
    SELECT i.message_id FROM channel_message_iatas i
    WHERE i.iata = req.iata AND i.message_id < @cursor_id::bigint
    ORDER BY i.message_id DESC
    LIMIT @page_limit) m
  GROUP BY m.message_id
  ORDER BY m.message_id DESC
  LIMIT @page_limit)
SELECT cm.id, cm.channel_id, cm.packet_hash, cm.sender_name, cm.sender_pubkey, cm.content, cm.sent_at,
       encode(cm.packet_hash, 'hex') AS packet_hash_hex, c.channel_hash, ts.name AS scope_name,
       p.transport_codes_present, p.observation_count AS observation_count
FROM hits h
JOIN channel_messages cm ON cm.id = h.message_id
JOIN channels c ON c.id = cm.channel_id
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY cm.id DESC;

-- name: ListAllChannelMessagesByIATAsScoped :many
-- Every channel filtered by sites and a scope: each (site, scope) range's newest message ids, merged.
WITH hits AS MATERIALIZED (
  SELECT m.message_id
  FROM unnest(@iatas::bpchar[]) AS req(iata)
  CROSS JOIN LATERAL (
    SELECT i.message_id FROM channel_message_iatas i
    WHERE i.iata = req.iata AND i.scope_id = @scope_id::int AND i.message_id < @cursor_id::bigint
    ORDER BY i.message_id DESC
    LIMIT @page_limit) m
  GROUP BY m.message_id
  ORDER BY m.message_id DESC
  LIMIT @page_limit)
SELECT cm.id, cm.channel_id, cm.packet_hash, cm.sender_name, cm.sender_pubkey, cm.content, cm.sent_at,
       encode(cm.packet_hash, 'hex') AS packet_hash_hex, c.channel_hash, ts.name AS scope_name,
       p.transport_codes_present, p.observation_count AS observation_count
FROM hits h
JOIN channel_messages cm ON cm.id = h.message_id
JOIN channels c ON c.id = cm.channel_id
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY cm.id DESC;

-- name: CountChannelMessagesSentSince :one
-- How many messages were sent at or after since, counted to at most row_limit: idx_channel_messages_sent alone.
SELECT count(*)::bigint AS n
FROM (SELECT 1 FROM channel_messages cm WHERE cm.sent_at >= @since::timestamptz LIMIT @row_limit::bigint) w;

-- name: ListAllChannelMessagesByIATAsSince :many
-- Every channel filtered by sites with since, optionally by scope, when few messages were sent since then: those
-- messages' ids (idx_channel_messages_sent), each kept when it has a row at one of the sites, newest first.
WITH sent AS MATERIALIZED (
  SELECT w.id, w.packet_hash FROM channel_messages w
  WHERE w.sent_at >= @since::timestamptz AND w.id < @cursor_id::bigint
), hits AS MATERIALIZED (
  SELECT s.id FROM sent s
  WHERE EXISTS (SELECT 1 FROM channel_message_iatas i WHERE i.message_id = s.id AND i.iata = ANY(@iatas::bpchar[]))
    AND (sqlc.narg('scope_id')::int IS NULL
         OR EXISTS (SELECT 1 FROM packets g WHERE g.packet_hash = s.packet_hash AND g.scope_id = sqlc.narg('scope_id')::int))
  ORDER BY s.id DESC
  LIMIT @page_limit
)
SELECT cm.id, cm.channel_id, cm.packet_hash, cm.sender_name, cm.sender_pubkey, cm.content, cm.sent_at,
       encode(cm.packet_hash, 'hex') AS packet_hash_hex, c.channel_hash, ts.name AS scope_name,
       p.transport_codes_present, p.observation_count AS observation_count
FROM hits h
JOIN channel_messages cm ON cm.id = h.id
JOIN channels c ON c.id = cm.channel_id
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY cm.id DESC;

-- name: ListAllChannelMessagesByIATAsSinceWalk :many
-- ListAllChannelMessagesByIATAsSince when many messages were sent since then: the newest ids down (or each site's
-- newest message ids, whichever the estimates find cheaper), keeping those sent since then.
SELECT cm.id, cm.channel_id, cm.packet_hash, cm.sender_name, cm.sender_pubkey, cm.content, cm.sent_at,
       encode(cm.packet_hash, 'hex') AS packet_hash_hex, c.channel_hash, ts.name AS scope_name,
       p.transport_codes_present, p.observation_count AS observation_count
FROM channel_messages cm
JOIN channels c ON c.id = cm.channel_id
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE cm.sent_at >= @since::timestamptz
  AND cm.id < @cursor_id::bigint
  AND EXISTS (SELECT 1 FROM channel_message_iatas i WHERE i.message_id = cm.id AND i.iata = ANY(@iatas::bpchar[]))
  AND (sqlc.narg('scope_id')::int IS NULL OR p.scope_id = sqlc.narg('scope_id')::int)
ORDER BY cm.id DESC
LIMIT @page_limit;

-- name: ListAllChannelMessagesPlainScoped :many
-- Every channel filtered by scope alone: the scope's newest message ids from the read model.
WITH hits AS MATERIALIZED (
  SELECT DISTINCT i.message_id
  FROM channel_message_iatas i
  WHERE i.scope_id = @scope_id::int AND i.message_id < @cursor_id::bigint
  ORDER BY i.message_id DESC
  LIMIT @page_limit)
SELECT cm.id, cm.channel_id, cm.packet_hash, cm.sender_name, cm.sender_pubkey, cm.content, cm.sent_at,
       encode(cm.packet_hash, 'hex') AS packet_hash_hex, c.channel_hash, ts.name AS scope_name,
       p.transport_codes_present, p.observation_count AS observation_count
FROM hits h
JOIN channel_messages cm ON cm.id = h.message_id
JOIN channels c ON c.id = cm.channel_id
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY cm.id DESC;

-- name: ListAllChannelMessagesPlain :many
-- Every channel, unfiltered: a walk by id; a message counts once it has a stored observation.
SELECT cm.id, cm.channel_id, cm.packet_hash, cm.sender_name, cm.sender_pubkey, cm.content, cm.sent_at,
       encode(cm.packet_hash, 'hex') AS packet_hash_hex, c.channel_hash, ts.name AS scope_name,
       p.transport_codes_present, p.observation_count AS observation_count
FROM channel_messages cm
JOIN channels c ON c.id = cm.channel_id
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE cm.id < @cursor_id::bigint
  AND EXISTS (SELECT 1 FROM packet_observations po WHERE po.packet_hash = cm.packet_hash)
ORDER BY cm.id DESC
LIMIT @page_limit;

-- name: ListAllChannelMessagesSince :many
-- Every channel with since, optionally by scope (no sites): a walk by id.
SELECT cm.id, cm.channel_id, cm.packet_hash, cm.sender_name, cm.sender_pubkey, cm.content, cm.sent_at,
       encode(cm.packet_hash, 'hex') AS packet_hash_hex, c.channel_hash, ts.name AS scope_name,
       p.transport_codes_present, p.observation_count AS observation_count
FROM channel_messages cm
JOIN channels c ON c.id = cm.channel_id
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE cm.sent_at >= @since::timestamptz
  AND (sqlc.narg('scope_id')::int IS NULL OR p.scope_id = sqlc.narg('scope_id')::int)
  AND cm.id < @cursor_id::bigint
  AND EXISTS (SELECT 1 FROM packet_observations po WHERE po.packet_hash = cm.packet_hash)
ORDER BY cm.id DESC
LIMIT @page_limit;

-- name: ListChannelMessageIATASamples :many
-- The busiest (channel, site) pairs among recent messages; the first-time verification compares their pages.
SELECT i.channel_id, i.iata, count(*)::bigint AS n
FROM channel_message_iatas i
WHERE i.message_id > @after_id::bigint
GROUP BY i.channel_id, i.iata
ORDER BY n DESC, i.channel_id, i.iata
LIMIT @row_limit;

-- name: ListChannelMessageScopeSamples :many
-- The busiest (channel, scope) pairs among recent messages, with the scope name today's statement filters on.
SELECT i.channel_id, i.scope_id, ts.name, count(*)::bigint AS n
FROM channel_message_iatas i
JOIN transport_scopes ts ON ts.id = i.scope_id
WHERE i.message_id > @after_id::bigint AND i.scope_id IS NOT NULL
GROUP BY i.channel_id, i.scope_id, ts.name
ORDER BY n DESC, i.channel_id, i.scope_id
LIMIT @row_limit;
