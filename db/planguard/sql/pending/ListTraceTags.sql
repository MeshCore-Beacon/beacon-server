-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: ListTraceTags :many

SELECT
    encode(t.trace_tag, 'hex') AS trace_tag,
    t.first_heard_at,
    t.last_heard_at,
    t.packet_count,
    (SELECT COUNT(*)
     FROM trace_iatas ti
     WHERE ti.trace_tag = t.trace_tag
       AND (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR ti.iata = ANY($1::bpchar[]))) AS iata_count,
    COALESCE(t.trace_type, '')::text AS trace_type,
    t.best_payload
FROM trace_tags t
WHERE (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR EXISTS (
        SELECT 1 FROM trace_iatas ti WHERE ti.trace_tag = t.trace_tag AND ti.iata = ANY($1::bpchar[])))
  AND ($2::text = '' OR t.scope_id = (SELECT id FROM transport_scopes WHERE name = $2))
  AND ($3::timestamptz IS NULL OR t.first_heard_at >= $3)
  AND ($4::timestamptz IS NULL OR t.first_heard_at <= $4)
  -- Keyset on the millisecond clients see, tie-broken by tag; matches idx_trace_tags_keyset.
  AND (date_trunc('milliseconds', t.last_heard_at, 'UTC'), t.trace_tag)
      < (COALESCE($5::timestamptz, 'infinity'::timestamptz), COALESCE($8::bytea, ''::bytea))
  AND ($7::text = '' OR t.trace_type = $7)
ORDER BY date_trunc('milliseconds', t.last_heard_at, 'UTC') DESC, t.trace_tag DESC
LIMIT $6
