-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: FillPacketSitesSlice :execrows
-- Each (packet, site) of the observations in (after_id, through_id], at its latest hearing there.
-- Pairs already at or past that hearing are skipped before the insert, so a clean slice writes nothing.
INSERT INTO packet_sites (packet_hash, iata, site_heard_at, first_heard_at, payload_type, route_type, scope_id)
SELECT g.packet_hash, g.iata, g.mx, g.first_heard_at, g.payload_type, g.route_type, g.scope_id
FROM (
  SELECT po.packet_hash, po.iata, max(po.heard_at) AS mx, p.first_heard_at, p.payload_type, p.route_type, p.scope_id
  FROM packet_observations po
  JOIN packets p ON p.packet_hash = po.packet_hash
  WHERE po.id > @after_id::bigint AND po.id <= @through_id::bigint
  GROUP BY po.packet_hash, po.iata, p.first_heard_at, p.payload_type, p.route_type, p.scope_id
) g
WHERE NOT EXISTS (SELECT 1 FROM packet_sites x
                  WHERE x.packet_hash = g.packet_hash AND x.iata = g.iata AND x.site_heard_at >= g.mx)
ON CONFLICT (packet_hash, iata) DO UPDATE SET site_heard_at = EXCLUDED.site_heard_at
  WHERE packet_sites.site_heard_at < EXCLUDED.site_heard_at;

-- name: MinObservationID :one
SELECT COALESCE(min(id), 0)::bigint AS id FROM packet_observations;

-- name: CheckPacketSitesSlice :one
-- Exact check of every packet with an observation in (after_id, through_id]: site rows the observations imply
-- and the table lacks, rows whose hearing time or packet columns differ, and rows no observation backs.
WITH pk AS MATERIALIZED (
  SELECT DISTINCT po.packet_hash FROM packet_observations po
  WHERE po.id > @after_id::bigint AND po.id <= @through_id::bigint
), def AS (
  SELECT po.packet_hash, po.iata, max(po.heard_at) AS site_heard_at,
         p.first_heard_at, p.payload_type, p.route_type, p.scope_id
  FROM pk
  JOIN packet_observations po ON po.packet_hash = pk.packet_hash
  JOIN packets p ON p.packet_hash = pk.packet_hash
  GROUP BY po.packet_hash, po.iata, p.first_heard_at, p.payload_type, p.route_type, p.scope_id
), have AS (
  SELECT ps.packet_hash, ps.iata, ps.site_heard_at, ps.first_heard_at, ps.payload_type, ps.route_type, ps.scope_id
  FROM pk JOIN packet_sites ps ON ps.packet_hash = pk.packet_hash
)
SELECT
  (SELECT count(*) FROM def d WHERE NOT EXISTS
     (SELECT 1 FROM have h WHERE h.packet_hash = d.packet_hash AND h.iata = d.iata))::bigint AS missing,
  (SELECT count(*) FROM def d JOIN have h ON h.packet_hash = d.packet_hash AND h.iata = d.iata
    WHERE (h.site_heard_at, h.first_heard_at, h.payload_type, h.route_type, h.scope_id)
          IS DISTINCT FROM (d.site_heard_at, d.first_heard_at, d.payload_type, d.route_type, d.scope_id))::bigint AS wrong,
  (SELECT count(*) FROM have h WHERE NOT EXISTS
     (SELECT 1 FROM def d WHERE d.packet_hash = h.packet_hash AND d.iata = h.iata))::bigint AS extra;

-- name: FilterKnownSites :one
-- The requested sites that have at least one row: a site with none adds no row to any site page.
SELECT COALESCE(array_agg(DISTINCT u ORDER BY u), '{}')::bpchar[] AS iatas
FROM unnest(@iatas::bpchar[]) AS u
WHERE EXISTS (SELECT 1 FROM packet_sites ps WHERE ps.iata = u);

-- name: ListPacketsBySite :many
-- One site: one range of its (site_heard_at, packet_hash) index.
WITH page AS MATERIALIZED (
  SELECT ps.packet_hash, ps.site_heard_at AS k
  FROM packet_sites ps
  WHERE ps.iata = @iata::bpchar
    AND (ps.site_heard_at, ps.packet_hash) < (@cursor_ts::timestamptz, @cursor_hash::bytea)
  ORDER BY ps.site_heard_at DESC, ps.packet_hash DESC
  LIMIT @page_limit
)
SELECT p.packet_hash, p.payload_type,
  COALESCE(CASE
    WHEN p.payload_type = 4 AND jsonb_typeof(p.parsed_payload #> '{appData,name}') = 'string'
      THEN p.parsed_payload #>> '{appData,name}'
    WHEN p.payload_type = 3 AND p.parsed_payload ->> 'type' = 'ACK'
      AND jsonb_typeof(p.parsed_payload -> 'checksum') = 'string'
      AND p.parsed_payload ->> 'checksum' ~ '^[0-9a-fA-F]{8}$'
      THEN 'ACK ' || lower(p.parsed_payload ->> 'checksum')
    WHEN p.payload_type = 9 AND p.parsed_payload ->> 'type' IN ('TRACE', 'PING')
      AND jsonb_typeof(p.parsed_payload -> 'traceTag') = 'string'
      AND p.parsed_payload ->> 'traceTag' ~ '^[0-9a-fA-F]{8}$'
      THEN (p.parsed_payload ->> 'type') || ' ' || lower(p.parsed_payload ->> 'traceTag')
    END, '')::text AS summary,
  p.route_type, p.first_heard_at, p.last_heard_at, p.scope_id, ts.name AS scope_name,
  pg.k::timestamptz AS site_heard_at,
  p.observation_count AS observation_count,
  -- sqlc loses LATERAL nullability; these scalar defaults are ignored when observer_id is NULL.
  po.observer_id AS latest_observer_id, o.display_name AS latest_observer_name,
  COALESCE(po.iata, ''::bpchar) AS latest_observer_iata,
  COALESCE(po.path_length_byte, 0::smallint) AS latest_observer_path_length_byte,
  COALESCE(po.hash_size, 0::smallint) AS latest_observer_hash_size,
  COALESCE(po.hop_count, 0::smallint) AS latest_observer_hop_count,
  po.path_bytes AS latest_observer_path_bytes, p.raw_payload, p.origin_pubkey
FROM page pg
JOIN packets p ON p.packet_hash = pg.packet_hash
LEFT JOIN LATERAL (
  SELECT observer_id, iata, path_length_byte, hash_size, hop_count, path_bytes
  FROM packet_observations WHERE packet_hash = p.packet_hash
  ORDER BY heard_at DESC, id DESC LIMIT 1
) po ON true
LEFT JOIN observers o ON o.id = po.observer_id
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY pg.k DESC, pg.packet_hash DESC;

-- name: ListPacketsBySites :many
-- Two or more sites with a cursor: one range per site; a packet whose newest requested-site row is at or above
-- the cursor was already returned, so the in-range re-check drops it.
WITH page AS MATERIALIZED (
  SELECT s.packet_hash, max(s.site_heard_at) AS k
  FROM unnest(@iatas::bpchar[]) AS req(iata)
  CROSS JOIN LATERAL (
    SELECT ps.packet_hash, ps.site_heard_at
    FROM packet_sites ps
    WHERE ps.iata = req.iata
      AND (ps.site_heard_at, ps.packet_hash) < (@cursor_ts::timestamptz, @cursor_hash::bytea)
      AND NOT EXISTS (SELECT 1 FROM packet_sites x
                      WHERE x.packet_hash = ps.packet_hash
                        AND x.iata = ANY(@iatas::bpchar[])
                        AND (x.site_heard_at, x.packet_hash) >= (@cursor_ts::timestamptz, @cursor_hash::bytea)
                      OFFSET 0)   -- a per-row SubPlan in both plan modes, never an anti join
    ORDER BY ps.site_heard_at DESC, ps.packet_hash DESC
    LIMIT @page_limit) s
  GROUP BY s.packet_hash
  ORDER BY max(s.site_heard_at) DESC, s.packet_hash DESC
  LIMIT @page_limit
)
SELECT p.packet_hash, p.payload_type,
  COALESCE(CASE
    WHEN p.payload_type = 4 AND jsonb_typeof(p.parsed_payload #> '{appData,name}') = 'string'
      THEN p.parsed_payload #>> '{appData,name}'
    WHEN p.payload_type = 3 AND p.parsed_payload ->> 'type' = 'ACK'
      AND jsonb_typeof(p.parsed_payload -> 'checksum') = 'string'
      AND p.parsed_payload ->> 'checksum' ~ '^[0-9a-fA-F]{8}$'
      THEN 'ACK ' || lower(p.parsed_payload ->> 'checksum')
    WHEN p.payload_type = 9 AND p.parsed_payload ->> 'type' IN ('TRACE', 'PING')
      AND jsonb_typeof(p.parsed_payload -> 'traceTag') = 'string'
      AND p.parsed_payload ->> 'traceTag' ~ '^[0-9a-fA-F]{8}$'
      THEN (p.parsed_payload ->> 'type') || ' ' || lower(p.parsed_payload ->> 'traceTag')
    END, '')::text AS summary,
  p.route_type, p.first_heard_at, p.last_heard_at, p.scope_id, ts.name AS scope_name,
  pg.k::timestamptz AS site_heard_at,
  p.observation_count AS observation_count,
  -- sqlc loses LATERAL nullability; these scalar defaults are ignored when observer_id is NULL.
  po.observer_id AS latest_observer_id, o.display_name AS latest_observer_name,
  COALESCE(po.iata, ''::bpchar) AS latest_observer_iata,
  COALESCE(po.path_length_byte, 0::smallint) AS latest_observer_path_length_byte,
  COALESCE(po.hash_size, 0::smallint) AS latest_observer_hash_size,
  COALESCE(po.hop_count, 0::smallint) AS latest_observer_hop_count,
  po.path_bytes AS latest_observer_path_bytes, p.raw_payload, p.origin_pubkey
FROM page pg
JOIN packets p ON p.packet_hash = pg.packet_hash
LEFT JOIN LATERAL (
  SELECT observer_id, iata, path_length_byte, hash_size, hop_count, path_bytes
  FROM packet_observations WHERE packet_hash = p.packet_hash
  ORDER BY heard_at DESC, id DESC LIMIT 1
) po ON true
LEFT JOIN observers o ON o.id = po.observer_id
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY pg.k DESC, pg.packet_hash DESC;

-- name: ListPacketsBySitesFirst :many
-- ListPacketsBySites for the first page or one site, where the re-check cannot match.
WITH page AS MATERIALIZED (
  SELECT s.packet_hash, max(s.site_heard_at) AS k
  FROM unnest(@iatas::bpchar[]) AS req(iata)
  CROSS JOIN LATERAL (
    SELECT ps.packet_hash, ps.site_heard_at
    FROM packet_sites ps
    WHERE ps.iata = req.iata
      AND (ps.site_heard_at, ps.packet_hash) < (@cursor_ts::timestamptz, @cursor_hash::bytea)
    ORDER BY ps.site_heard_at DESC, ps.packet_hash DESC
    LIMIT @page_limit) s
  GROUP BY s.packet_hash
  ORDER BY max(s.site_heard_at) DESC, s.packet_hash DESC
  LIMIT @page_limit
)
SELECT p.packet_hash, p.payload_type,
  COALESCE(CASE
    WHEN p.payload_type = 4 AND jsonb_typeof(p.parsed_payload #> '{appData,name}') = 'string'
      THEN p.parsed_payload #>> '{appData,name}'
    WHEN p.payload_type = 3 AND p.parsed_payload ->> 'type' = 'ACK'
      AND jsonb_typeof(p.parsed_payload -> 'checksum') = 'string'
      AND p.parsed_payload ->> 'checksum' ~ '^[0-9a-fA-F]{8}$'
      THEN 'ACK ' || lower(p.parsed_payload ->> 'checksum')
    WHEN p.payload_type = 9 AND p.parsed_payload ->> 'type' IN ('TRACE', 'PING')
      AND jsonb_typeof(p.parsed_payload -> 'traceTag') = 'string'
      AND p.parsed_payload ->> 'traceTag' ~ '^[0-9a-fA-F]{8}$'
      THEN (p.parsed_payload ->> 'type') || ' ' || lower(p.parsed_payload ->> 'traceTag')
    END, '')::text AS summary,
  p.route_type, p.first_heard_at, p.last_heard_at, p.scope_id, ts.name AS scope_name,
  pg.k::timestamptz AS site_heard_at,
  p.observation_count AS observation_count,
  -- sqlc loses LATERAL nullability; these scalar defaults are ignored when observer_id is NULL.
  po.observer_id AS latest_observer_id, o.display_name AS latest_observer_name,
  COALESCE(po.iata, ''::bpchar) AS latest_observer_iata,
  COALESCE(po.path_length_byte, 0::smallint) AS latest_observer_path_length_byte,
  COALESCE(po.hash_size, 0::smallint) AS latest_observer_hash_size,
  COALESCE(po.hop_count, 0::smallint) AS latest_observer_hop_count,
  po.path_bytes AS latest_observer_path_bytes, p.raw_payload, p.origin_pubkey
FROM page pg
JOIN packets p ON p.packet_hash = pg.packet_hash
LEFT JOIN LATERAL (
  SELECT observer_id, iata, path_length_byte, hash_size, hop_count, path_bytes
  FROM packet_observations WHERE packet_hash = p.packet_hash
  ORDER BY heard_at DESC, id DESC LIMIT 1
) po ON true
LEFT JOIN observers o ON o.id = po.observer_id
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY pg.k DESC, pg.packet_hash DESC;

-- name: ListPacketsBySitesTyped :many
-- One range per (site, payload type, route type); Go passes each type list deduplicated and sends only a few
-- type pairs here.
WITH page AS MATERIALIZED (
  SELECT s.packet_hash, max(s.site_heard_at) AS k
  FROM unnest(@iatas::bpchar[]) AS req(iata)
  CROSS JOIN unnest(@payload_types::smallint[]) AS pt(t)
  CROSS JOIN unnest(@route_types::smallint[]) AS rt(t)
  CROSS JOIN LATERAL (
    SELECT ps.packet_hash, ps.site_heard_at
    FROM packet_sites ps
    WHERE ps.iata = req.iata AND ps.payload_type = pt.t AND ps.route_type = rt.t
      AND (ps.site_heard_at, ps.packet_hash) < (@cursor_ts::timestamptz, @cursor_hash::bytea)
      AND NOT EXISTS (SELECT 1 FROM packet_sites x
                      WHERE x.packet_hash = ps.packet_hash
                        AND x.iata = ANY(@iatas::bpchar[])
                        AND (x.site_heard_at, x.packet_hash) >= (@cursor_ts::timestamptz, @cursor_hash::bytea)
                      OFFSET 0)   -- a per-row SubPlan in both plan modes, never an anti join
    ORDER BY ps.site_heard_at DESC, ps.packet_hash DESC
    LIMIT @page_limit) s
  GROUP BY s.packet_hash
  ORDER BY max(s.site_heard_at) DESC, s.packet_hash DESC
  LIMIT @page_limit
)
SELECT p.packet_hash, p.payload_type,
  COALESCE(CASE
    WHEN p.payload_type = 4 AND jsonb_typeof(p.parsed_payload #> '{appData,name}') = 'string'
      THEN p.parsed_payload #>> '{appData,name}'
    WHEN p.payload_type = 3 AND p.parsed_payload ->> 'type' = 'ACK'
      AND jsonb_typeof(p.parsed_payload -> 'checksum') = 'string'
      AND p.parsed_payload ->> 'checksum' ~ '^[0-9a-fA-F]{8}$'
      THEN 'ACK ' || lower(p.parsed_payload ->> 'checksum')
    WHEN p.payload_type = 9 AND p.parsed_payload ->> 'type' IN ('TRACE', 'PING')
      AND jsonb_typeof(p.parsed_payload -> 'traceTag') = 'string'
      AND p.parsed_payload ->> 'traceTag' ~ '^[0-9a-fA-F]{8}$'
      THEN (p.parsed_payload ->> 'type') || ' ' || lower(p.parsed_payload ->> 'traceTag')
    END, '')::text AS summary,
  p.route_type, p.first_heard_at, p.last_heard_at, p.scope_id, ts.name AS scope_name,
  pg.k::timestamptz AS site_heard_at,
  p.observation_count AS observation_count,
  -- sqlc loses LATERAL nullability; these scalar defaults are ignored when observer_id is NULL.
  po.observer_id AS latest_observer_id, o.display_name AS latest_observer_name,
  COALESCE(po.iata, ''::bpchar) AS latest_observer_iata,
  COALESCE(po.path_length_byte, 0::smallint) AS latest_observer_path_length_byte,
  COALESCE(po.hash_size, 0::smallint) AS latest_observer_hash_size,
  COALESCE(po.hop_count, 0::smallint) AS latest_observer_hop_count,
  po.path_bytes AS latest_observer_path_bytes, p.raw_payload, p.origin_pubkey
FROM page pg
JOIN packets p ON p.packet_hash = pg.packet_hash
LEFT JOIN LATERAL (
  SELECT observer_id, iata, path_length_byte, hash_size, hop_count, path_bytes
  FROM packet_observations WHERE packet_hash = p.packet_hash
  ORDER BY heard_at DESC, id DESC LIMIT 1
) po ON true
LEFT JOIN observers o ON o.id = po.observer_id
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY pg.k DESC, pg.packet_hash DESC;

-- name: ListPacketsBySitesTypedFirst :many
-- ListPacketsBySitesTyped for the first page or one site.
WITH page AS MATERIALIZED (
  SELECT s.packet_hash, max(s.site_heard_at) AS k
  FROM unnest(@iatas::bpchar[]) AS req(iata)
  CROSS JOIN unnest(@payload_types::smallint[]) AS pt(t)
  CROSS JOIN unnest(@route_types::smallint[]) AS rt(t)
  CROSS JOIN LATERAL (
    SELECT ps.packet_hash, ps.site_heard_at
    FROM packet_sites ps
    WHERE ps.iata = req.iata AND ps.payload_type = pt.t AND ps.route_type = rt.t
      AND (ps.site_heard_at, ps.packet_hash) < (@cursor_ts::timestamptz, @cursor_hash::bytea)
    ORDER BY ps.site_heard_at DESC, ps.packet_hash DESC
    LIMIT @page_limit) s
  GROUP BY s.packet_hash
  ORDER BY max(s.site_heard_at) DESC, s.packet_hash DESC
  LIMIT @page_limit
)
SELECT p.packet_hash, p.payload_type,
  COALESCE(CASE
    WHEN p.payload_type = 4 AND jsonb_typeof(p.parsed_payload #> '{appData,name}') = 'string'
      THEN p.parsed_payload #>> '{appData,name}'
    WHEN p.payload_type = 3 AND p.parsed_payload ->> 'type' = 'ACK'
      AND jsonb_typeof(p.parsed_payload -> 'checksum') = 'string'
      AND p.parsed_payload ->> 'checksum' ~ '^[0-9a-fA-F]{8}$'
      THEN 'ACK ' || lower(p.parsed_payload ->> 'checksum')
    WHEN p.payload_type = 9 AND p.parsed_payload ->> 'type' IN ('TRACE', 'PING')
      AND jsonb_typeof(p.parsed_payload -> 'traceTag') = 'string'
      AND p.parsed_payload ->> 'traceTag' ~ '^[0-9a-fA-F]{8}$'
      THEN (p.parsed_payload ->> 'type') || ' ' || lower(p.parsed_payload ->> 'traceTag')
    END, '')::text AS summary,
  p.route_type, p.first_heard_at, p.last_heard_at, p.scope_id, ts.name AS scope_name,
  pg.k::timestamptz AS site_heard_at,
  p.observation_count AS observation_count,
  -- sqlc loses LATERAL nullability; these scalar defaults are ignored when observer_id is NULL.
  po.observer_id AS latest_observer_id, o.display_name AS latest_observer_name,
  COALESCE(po.iata, ''::bpchar) AS latest_observer_iata,
  COALESCE(po.path_length_byte, 0::smallint) AS latest_observer_path_length_byte,
  COALESCE(po.hash_size, 0::smallint) AS latest_observer_hash_size,
  COALESCE(po.hop_count, 0::smallint) AS latest_observer_hop_count,
  po.path_bytes AS latest_observer_path_bytes, p.raw_payload, p.origin_pubkey
FROM page pg
JOIN packets p ON p.packet_hash = pg.packet_hash
LEFT JOIN LATERAL (
  SELECT observer_id, iata, path_length_byte, hash_size, hop_count, path_bytes
  FROM packet_observations WHERE packet_hash = p.packet_hash
  ORDER BY heard_at DESC, id DESC LIMIT 1
) po ON true
LEFT JOIN observers o ON o.id = po.observer_id
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY pg.k DESC, pg.packet_hash DESC;

-- name: ListPacketsBySitesSince :many
-- Page keys for a first_heard_at window or many type pairs (absent window sides are -infinity and infinity).
-- Each site range is read to at most scan_depth entries, as deep as the observation statement reads, and the
-- window and type tests filter what was read; the last row (is_floor) is the floor below which this page is
-- incomplete, when a range filled scan_depth. Rows below the floor are left to the next page.
WITH scanned AS MATERIALIZED (
  SELECT w.*
  FROM unnest(@iatas::bpchar[]) AS req(iata)
  CROSS JOIN LATERAL (
    SELECT ps.iata, ps.scope_id, ps.packet_hash, ps.site_heard_at, ps.first_heard_at, ps.payload_type, ps.route_type
    FROM packet_sites ps
    WHERE ps.iata = req.iata
      AND (ps.site_heard_at, ps.packet_hash) < (@cursor_ts::timestamptz, @cursor_hash::bytea)
    ORDER BY ps.site_heard_at DESC, ps.packet_hash DESC
    LIMIT @scan_depth) w
  -- No packet was first heard at or after since: nothing can match, so no range is read. A packet's last_heard_at
  -- is at or after its first_heard_at, except the rows idx_packets_heard_inverted holds.
  -- Both maxima are one index probe each.
  WHERE (SELECT max(g.last_heard_at) FROM packets g) >= @since_ts::timestamptz
     OR (SELECT max(g.first_heard_at) FROM packets g WHERE (g.first_heard_at > g.last_heard_at) IS TRUE) >= @since_ts::timestamptz
), floor AS (
  -- Every range that filled scan_depth has unread entries below its last one; the highest such entry bounds
  -- what this page knows exactly.
  SELECT f.site_heard_at, f.packet_hash
  FROM (SELECT DISTINCT ON (w.iata) w.site_heard_at, w.packet_hash
        FROM scanned w
        WHERE (w.iata) IN (SELECT w.iata FROM scanned w GROUP BY w.iata HAVING count(*) >= @scan_depth)
        ORDER BY w.iata, w.site_heard_at, w.packet_hash) f
  ORDER BY f.site_heard_at DESC, f.packet_hash DESC
  LIMIT 1
), page AS (
  SELECT s.packet_hash, max(s.site_heard_at) AS k
  FROM scanned s
  WHERE s.first_heard_at >= @since_ts::timestamptz AND s.first_heard_at <= @until_ts::timestamptz
    AND s.payload_type = ANY(@payload_types::smallint[]) AND s.route_type = ANY(@route_types::smallint[])
  GROUP BY s.packet_hash
  HAVING NOT EXISTS (SELECT 1 FROM floor f WHERE (max(s.site_heard_at), s.packet_hash) < (f.site_heard_at, f.packet_hash))
    AND (NOT @recheck::boolean OR NOT EXISTS (
      SELECT 1 FROM packet_sites x
      WHERE x.packet_hash = s.packet_hash
        AND x.iata = ANY(@iatas::bpchar[])
        AND (x.site_heard_at, x.packet_hash) >= (@cursor_ts::timestamptz, @cursor_hash::bytea)))
  ORDER BY max(s.site_heard_at) DESC, s.packet_hash DESC
  LIMIT @page_limit
)
SELECT false AS is_floor, pg.packet_hash, pg.k::timestamptz AS site_heard_at FROM page pg
UNION ALL
SELECT true, f.packet_hash, f.site_heard_at FROM floor f
ORDER BY 1, 3 DESC, 2 DESC;

-- name: ListPacketsBySitesScoped :many
-- ListPacketsBySitesSince with one range per (site, named scope).
WITH scanned AS MATERIALIZED (
  SELECT w.*
  FROM unnest(@iatas::bpchar[]) AS req(iata)
  CROSS JOIN (SELECT id FROM transport_scopes WHERE name = ANY(@scope_names::text[])) sc
  CROSS JOIN LATERAL (
    SELECT ps.iata, ps.scope_id, ps.packet_hash, ps.site_heard_at, ps.first_heard_at, ps.payload_type, ps.route_type
    FROM packet_sites ps
    WHERE ps.iata = req.iata AND ps.scope_id = sc.id
      AND (ps.site_heard_at, ps.packet_hash) < (@cursor_ts::timestamptz, @cursor_hash::bytea)
    ORDER BY ps.site_heard_at DESC, ps.packet_hash DESC
    LIMIT @scan_depth) w
  -- No packet was first heard at or after since: nothing can match, so no range is read. A packet's last_heard_at
  -- is at or after its first_heard_at, except the rows idx_packets_heard_inverted holds.
  -- Both maxima are one index probe each.
  WHERE (SELECT max(g.last_heard_at) FROM packets g) >= @since_ts::timestamptz
     OR (SELECT max(g.first_heard_at) FROM packets g WHERE (g.first_heard_at > g.last_heard_at) IS TRUE) >= @since_ts::timestamptz
), floor AS (
  -- Every range that filled scan_depth has unread entries below its last one; the highest such entry bounds
  -- what this page knows exactly.
  SELECT f.site_heard_at, f.packet_hash
  FROM (SELECT DISTINCT ON (w.iata, w.scope_id) w.site_heard_at, w.packet_hash
        FROM scanned w
        WHERE (w.iata, w.scope_id) IN (SELECT w.iata, w.scope_id FROM scanned w GROUP BY w.iata, w.scope_id HAVING count(*) >= @scan_depth)
        ORDER BY w.iata, w.scope_id, w.site_heard_at, w.packet_hash) f
  ORDER BY f.site_heard_at DESC, f.packet_hash DESC
  LIMIT 1
), page AS (
  SELECT s.packet_hash, max(s.site_heard_at) AS k
  FROM scanned s
  WHERE s.first_heard_at >= @since_ts::timestamptz AND s.first_heard_at <= @until_ts::timestamptz
    AND s.payload_type = ANY(@payload_types::smallint[]) AND s.route_type = ANY(@route_types::smallint[])
  GROUP BY s.packet_hash
  HAVING NOT EXISTS (SELECT 1 FROM floor f WHERE (max(s.site_heard_at), s.packet_hash) < (f.site_heard_at, f.packet_hash))
    AND (NOT @recheck::boolean OR NOT EXISTS (
      SELECT 1 FROM packet_sites x
      WHERE x.packet_hash = s.packet_hash
        AND x.iata = ANY(@iatas::bpchar[])
        AND (x.site_heard_at, x.packet_hash) >= (@cursor_ts::timestamptz, @cursor_hash::bytea)))
  ORDER BY max(s.site_heard_at) DESC, s.packet_hash DESC
  LIMIT @page_limit
)
SELECT false AS is_floor, pg.packet_hash, pg.k::timestamptz AS site_heard_at FROM page pg
UNION ALL
SELECT true, f.packet_hash, f.site_heard_at FROM floor f
ORDER BY 1, 3 DESC, 2 DESC;

-- name: ListPacketsBySiteKeys :many
-- The page rows for keys from ListPacketsBySitesSince or ListPacketsBySitesScoped, in the given order.
WITH page AS MATERIALIZED (
  SELECT h.packet_hash, t.k, h.ord
  FROM unnest(@packet_hashes::bytea[]) WITH ORDINALITY AS h(packet_hash, ord)
  JOIN unnest(@site_heard_ats::timestamptz[]) WITH ORDINALITY AS t(k, ord) ON t.ord = h.ord
)
SELECT p.packet_hash, p.payload_type,
  COALESCE(CASE
    WHEN p.payload_type = 4 AND jsonb_typeof(p.parsed_payload #> '{appData,name}') = 'string'
      THEN p.parsed_payload #>> '{appData,name}'
    WHEN p.payload_type = 3 AND p.parsed_payload ->> 'type' = 'ACK'
      AND jsonb_typeof(p.parsed_payload -> 'checksum') = 'string'
      AND p.parsed_payload ->> 'checksum' ~ '^[0-9a-fA-F]{8}$'
      THEN 'ACK ' || lower(p.parsed_payload ->> 'checksum')
    WHEN p.payload_type = 9 AND p.parsed_payload ->> 'type' IN ('TRACE', 'PING')
      AND jsonb_typeof(p.parsed_payload -> 'traceTag') = 'string'
      AND p.parsed_payload ->> 'traceTag' ~ '^[0-9a-fA-F]{8}$'
      THEN (p.parsed_payload ->> 'type') || ' ' || lower(p.parsed_payload ->> 'traceTag')
    END, '')::text AS summary,
  p.route_type, p.first_heard_at, p.last_heard_at, p.scope_id, ts.name AS scope_name,
  pg.k::timestamptz AS site_heard_at,
  p.observation_count AS observation_count,
  -- sqlc loses LATERAL nullability; these scalar defaults are ignored when observer_id is NULL.
  po.observer_id AS latest_observer_id, o.display_name AS latest_observer_name,
  COALESCE(po.iata, ''::bpchar) AS latest_observer_iata,
  COALESCE(po.path_length_byte, 0::smallint) AS latest_observer_path_length_byte,
  COALESCE(po.hash_size, 0::smallint) AS latest_observer_hash_size,
  COALESCE(po.hop_count, 0::smallint) AS latest_observer_hop_count,
  po.path_bytes AS latest_observer_path_bytes, p.raw_payload, p.origin_pubkey
FROM page pg
JOIN packets p ON p.packet_hash = pg.packet_hash
LEFT JOIN LATERAL (
  SELECT observer_id, iata, path_length_byte, hash_size, hop_count, path_bytes
  FROM packet_observations WHERE packet_hash = p.packet_hash
  ORDER BY heard_at DESC, id DESC LIMIT 1
) po ON true
LEFT JOIN observers o ON o.id = po.observer_id
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY pg.ord;

-- name: SweepPacketSitesFronts :one
-- Read-only. Visits, through each time-ordered packet_sites index, every entry whose key was written in the last
-- lookback (2 minutes). An index scan that reaches an entry whose heap tuple is dead to every snapshot sets the
-- entry's LP_DEAD hint; when that leaf later fills, the inserting backend deletes hinted entries instead of splitting
-- (nbtree simple deletion). Keeps the three indexes at about 2x a fresh build instead of 4-9x. Changes no row.
-- The ORDER BY ... LIMIT keeps every lateral a per-front index range scan (never flattened into a join).
-- The cap is far above any front's 2-minute volume (the busiest site: about 150 entries) and only bounds the gauge.
WITH site AS MATERIALIZED (
  SELECT ps.iata, ps.payload_type, ps.route_type, ps.scope_id
  FROM iata_codes c
  CROSS JOIN LATERAL (
    SELECT p.iata, p.payload_type, p.route_type, p.scope_id
    FROM packet_sites p
    WHERE p.iata = c.iata AND p.site_heard_at > now() - @lookback::interval
    ORDER BY p.site_heard_at DESC LIMIT 100000) ps                   -- idx_packet_sites_site, Index Scan
), typ AS (
  SELECT count(*) AS n
  FROM (SELECT DISTINCT iata, payload_type, route_type FROM site) f
  CROSS JOIN LATERAL (
    SELECT 1 FROM packet_sites p
    WHERE p.iata = f.iata AND p.payload_type = f.payload_type AND p.route_type = f.route_type
      AND p.site_heard_at > now() - @lookback::interval
    ORDER BY p.site_heard_at DESC LIMIT 100000) x                    -- idx_packet_sites_site_type, Index Only Scan
), scp AS (
  SELECT count(*) AS n
  FROM (SELECT DISTINCT iata, scope_id FROM site WHERE scope_id IS NOT NULL) f
  CROSS JOIN LATERAL (
    SELECT 1 FROM packet_sites p
    WHERE p.iata = f.iata AND p.scope_id = f.scope_id AND p.scope_id IS NOT NULL
      AND p.site_heard_at > now() - @lookback::interval
    ORDER BY p.site_heard_at DESC LIMIT 100000) x                    -- idx_packet_sites_site_scope, Index Only Scan
)
SELECT (SELECT count(*) FROM site) AS site_live, (SELECT n FROM typ) AS type_live, (SELECT n FROM scp) AS scope_live;

-- name: PacketSitesTotalBytes :one
SELECT pg_total_relation_size('packet_sites'::regclass)::bigint AS bytes;

-- name: ListPacketSiteSamples :many
-- The sites with the most packets among observations above after_id; the first-time verification pages them.
SELECT po.iata, count(DISTINCT po.packet_hash)::bigint AS n
FROM packet_observations po
WHERE po.id > @after_id::bigint
GROUP BY po.iata
ORDER BY n DESC, po.iata
LIMIT @row_limit;

-- name: TruthPacketsBySitesPage :many
-- Page keys straight from the observations, independent of packet_sites. floor_ts bounds the read: keys at or
-- above it are exact, because a packet's newest requested-site hearing is unchanged by dropping older ones.
WITH page AS MATERIALIZED (
  SELECT po.packet_hash, max(po.heard_at) AS k
  FROM packet_observations po
  WHERE po.iata = ANY(@iatas::bpchar[]) AND po.heard_at >= @floor_ts::timestamptz
  GROUP BY po.packet_hash
  HAVING (max(po.heard_at), po.packet_hash) < (@cursor_ts::timestamptz, @cursor_hash::bytea)
  ORDER BY max(po.heard_at) DESC, po.packet_hash DESC
  LIMIT @page_limit
)
SELECT pg.packet_hash, pg.k::timestamptz AS site_heard_at FROM page pg ORDER BY pg.k DESC, pg.packet_hash DESC;
