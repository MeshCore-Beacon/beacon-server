-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: OraclePacketsWindow
-- The global /packets filter as the single ListPackets statement applied it before the per-shape statements
-- (an empty or NULL list, since or until is no filter), in the exact (last_heard_at, packet_hash) order and from an
-- exact row cursor: the truth a cursor walk is compared with. Its params sit where
-- ListPacketsWindow and ListPacketsWindowScoped have their cursor and limit:
-- $1 since, $2 ts, $3 hash, $4 until, $5 pt, $6 rt, $7 limit, $8 scopes.
SELECT p.packet_hash, p.last_heard_at
FROM packets p
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE (COALESCE(cardinality($5::smallint[]), 0) = 0 OR p.payload_type = ANY($5::smallint[]))
  AND (COALESCE(cardinality($6::smallint[]), 0) = 0 OR p.route_type = ANY($6::smallint[]))
  AND ($1::timestamptz IS NULL OR p.first_heard_at >= $1::timestamptz)
  AND ($4::timestamptz IS NULL OR p.first_heard_at <= $4::timestamptz)
  AND (p.last_heard_at, p.packet_hash) < ($2::timestamptz, $3::bytea)
  AND (COALESCE(cardinality($8::text[]), 0) = 0 OR ts.name = ANY($8::text[]))
ORDER BY p.last_heard_at DESC, p.packet_hash DESC
LIMIT $7::int
