-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: ListObservers :many
SELECT
  o.id,
  o.display_name,
  o.observer_type,
  o.last_status_at,
  o.radio_freq_mhz,
  o.radio_sf,
  o.radio_bw_khz,
  array_remove(array_agg(DISTINCT ts.name ORDER BY ts.name), NULL)::text[] AS scopes,
COALESCE(CASE
    WHEN GREATEST(COALESCE(o.last_status_at, o.last_seen), o.last_seen) > NOW() - INTERVAL '5 minutes' THEN 'online'
    ELSE 'offline'
END, 'offline')::text AS status,
COALESCE(o.last_iata, '')::text AS iata,
o.last_seen
FROM observers o
LEFT JOIN observer_brokers ob ON ob.observer_id = o.id
LEFT JOIN observer_scopes os ON os.observer_id = o.id
LEFT JOIN transport_scopes ts ON ts.id = os.scope_id
WHERE
  (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR o.last_iata = ANY($1::bpchar[]))
  AND ($2 = '' OR o.observer_type = $2)
  AND ($3 = '' OR ob.broker_name = $3)
  AND ($4 = '' OR CASE
    WHEN GREATEST(COALESCE(o.last_status_at, o.last_seen), o.last_seen) > NOW() - INTERVAL '5 minutes' THEN 'online'
    ELSE 'offline'
  END = $4)
  AND ($5 = '' OR o.display_name ILIKE '%' || $5 || '%')
  AND (o.last_seen, o.id) < ($6::timestamptz, $9::uuid)
  AND ($8::text = '' OR EXISTS (
    SELECT 1 FROM observer_scopes os2
    JOIN transport_scopes ts2 ON ts2.id = os2.scope_id
    WHERE os2.observer_id = o.id AND ts2.name = $8::text
  ))
GROUP BY o.id
ORDER BY o.last_seen DESC, o.id DESC
LIMIT $7
