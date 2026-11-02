-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: OraclePacketsPage
-- The global /packets filter as the single ListPackets statement applied it before the per-shape statements
-- (an empty or NULL list, since or until is no filter), in the exact (last_heard_at, packet_hash) order and from an
-- exact row cursor: the truth a cursor walk is compared with. Its params sit where
-- ListPacketsPage and ListPacketsByScopes have their cursor and limit:
-- $1 ts, $2 hash, $3 limit, $4 scopes, $5 pt, $6 rt, $7 since, $8 until.
SELECT p.packet_hash, p.last_heard_at
FROM packets p
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE (COALESCE(cardinality($5::smallint[]), 0) = 0 OR p.payload_type = ANY($5::smallint[]))
  AND (COALESCE(cardinality($6::smallint[]), 0) = 0 OR p.route_type = ANY($6::smallint[]))
  AND ($7::timestamptz IS NULL OR p.first_heard_at >= $7::timestamptz)
  AND ($8::timestamptz IS NULL OR p.first_heard_at <= $8::timestamptz)
  AND (p.last_heard_at, p.packet_hash) < ($1::timestamptz, $2::bytea)
  AND (COALESCE(cardinality($4::text[]), 0) = 0 OR ts.name = ANY($4::text[]))
ORDER BY p.last_heard_at DESC, p.packet_hash DESC
LIMIT $3::int
