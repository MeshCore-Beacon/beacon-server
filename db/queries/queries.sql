-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: agpl

-- ============================================================
-- IATA CODES
-- ============================================================

-- name: UpsertIATA :exec
INSERT INTO iata_codes (iata)
VALUES ($1)
ON CONFLICT (iata) DO NOTHING;

-- name: GetIATA :one
SELECT * FROM iata_codes WHERE iata = $1;

-- name: ListIATAs :many
SELECT * FROM iata_codes ORDER BY iata;

-- name: UpsertIATADetails :exec
INSERT INTO iata_codes (iata, display_name, approx_lat, approx_lng)
VALUES ($1, $2, $3, $4)
ON CONFLICT (iata) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    approx_lat   = EXCLUDED.approx_lat,
    approx_lng   = EXCLUDED.approx_lng;

-- name: GetIATABorder :one
-- border is NULL when the IATA exists but has no border configured; a
-- missing row (unknown IATA) is sql.ErrNoRows, same not-found distinction
-- GetIATA already makes.
SELECT border FROM iata_codes WHERE iata = $1;

-- name: UpsertIATABorder :exec
-- Written by the config-file-driven seeder (internal/config/seed.go), not a
-- runtime HTTP path. border is a full, pre-validated GeoJSON Feature with
-- bbox already computed -- see internal/config/border.go.
INSERT INTO iata_codes (iata, border)
VALUES ($1, $2)
ON CONFLICT (iata) DO UPDATE SET
    border = EXCLUDED.border;

-- ============================================================
-- TRANSPORT CODES
-- ============================================================

-- name: UpsertTransportScope :exec
INSERT INTO transport_scopes (name, display_name, transport_key, key_fingerprint)
VALUES ($1, $2, $3, $4)
ON CONFLICT (name) DO UPDATE SET
  display_name    = EXCLUDED.display_name,
  transport_key   = EXCLUDED.transport_key,
  key_fingerprint = EXCLUDED.key_fingerprint;

-- name: GetTransportScopes :many
SELECT name, transport_key, key_fingerprint FROM transport_scopes ORDER BY name;

-- name: GetTransportScopeByName :one
SELECT id FROM transport_scopes WHERE name = $1;

-- name: GetScopeNames :many
SELECT name FROM transport_scopes ORDER BY name;

-- name: GetScopesByIATAs :many
SELECT
    ts.name,
    COUNT(DISTINCT os.observer_id) AS observer_count,
    COUNT(DISTINCT n.id) AS node_count,
    COUNT(DISTINCT po.iata) AS iata_count
FROM transport_scopes ts
LEFT JOIN observer_scopes os ON os.scope_id = ts.id
LEFT JOIN observers o ON o.id = os.observer_id
LEFT JOIN packet_observations po ON po.observer_id = o.id
LEFT JOIN nodes n ON n.default_scope_id = ts.id
WHERE (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR po.iata = ANY($1::bpchar[]))
GROUP BY ts.name
ORDER BY ts.name;

-- name: GetScopeByName :one
SELECT
    ts.name,
    COUNT(DISTINCT p.packet_hash) AS packet_count,
    COUNT(DISTINCT os.observer_id) AS observer_count,
    COUNT(DISTINCT n.id) AS node_count,
    COUNT(DISTINCT po.iata) AS iata_count,
    array_remove(array_agg(DISTINCT po.iata ORDER BY po.iata), NULL)::text[] AS iatas
FROM transport_scopes ts
LEFT JOIN packets p ON p.scope_id = ts.id
LEFT JOIN observer_scopes os ON os.scope_id = ts.id
LEFT JOIN observers o ON o.id = os.observer_id
LEFT JOIN packet_observations po ON po.observer_id = o.id
LEFT JOIN nodes n ON n.default_scope_id = ts.id
WHERE ts.name = $1
GROUP BY ts.name;

-- ============================================================
-- OBSERVERS
-- ============================================================

-- name: UpsertObserver :one
INSERT INTO observers (public_key, observer_type, last_seen)
VALUES ($1, 'unknown', NOW())
ON CONFLICT (public_key) DO UPDATE SET
  last_seen         = NOW(),
  observation_count = observers.observation_count + 1
RETURNING *;

-- name: UpdateObserverStatus :one
UPDATE observers SET
  display_name     = COALESCE(NULLIF($2, ''), display_name),
  observer_type    = COALESCE(NULLIF($3, ''), observer_type),
  software_version = COALESCE($4, software_version),
  hardware_model   = COALESCE($5, hardware_model),
  firmware_version = COALESCE($6, firmware_version),
  firmware_build   = COALESCE($7, firmware_build),
  radio_freq_mhz   = COALESCE($8, radio_freq_mhz),
  radio_sf         = COALESCE($9, radio_sf),
  radio_bw_khz     = COALESCE($10, radio_bw_khz),
  radio_cr         = COALESCE($11, radio_cr),
  battery_level    = COALESCE($12, battery_level),
  uptime_seconds   = COALESCE($13, uptime_seconds),
  status_metadata  = $14,
  last_status_at   = NOW(),
  last_seen        = NOW()
WHERE public_key = $1
RETURNING id;

-- name: TouchObservers :exec
-- Batched flush of coalesced presence bumps. GREATEST keeps a late flush
-- from regressing a newer write-through (e.g. a status update).
UPDATE observers o SET
  last_seen         = GREATEST(o.last_seen, v.seen),
  observation_count = COALESCE(o.observation_count, 0) + v.delta
FROM (
  SELECT unnest($1::uuid[]) AS id,
         unnest($2::timestamptz[]) AS seen,
         unnest($3::int[]) AS delta
) v
WHERE o.id = v.id;

-- name: UpsertObserverScope :exec
INSERT INTO observer_scopes (observer_id, scope_id, last_seen)
VALUES ($1, $2, NOW())
ON CONFLICT (observer_id, scope_id) DO UPDATE SET
  last_seen = NOW();

-- name: GetObserverScopes :many
SELECT ts.name FROM observer_scopes os
JOIN transport_scopes ts ON ts.id = os.scope_id
WHERE os.observer_id = $1
ORDER BY ts.name;

-- name: GetObserverByPubkey :one
SELECT * FROM observers WHERE public_key = $1;

-- name: GetObserverByID :one
SELECT * FROM observers WHERE id = $1;

-- name: GetObserverBrokers :many
SELECT broker_name, last_seen, last_packet_at
FROM observer_brokers
WHERE observer_id = $1
ORDER BY last_seen DESC;

-- name: ListObservers :many
-- Pass cursor=0 to start from the beginning, or the last seen observer's rownum for pagination.
-- Note: observers use UUID PKs so we order by last_seen and use a keyset on last_seen+id.
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
COALESCE((
    SELECT po.iata
    FROM packet_observations po
    WHERE po.observer_id = o.id
    ORDER BY po.heard_at DESC
    LIMIT 1
), '')::text AS iata
FROM observers o
LEFT JOIN observer_brokers ob ON ob.observer_id = o.id
LEFT JOIN observer_scopes os ON os.observer_id = o.id
LEFT JOIN transport_scopes ts ON ts.id = os.scope_id
WHERE
  (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR (
      SELECT po.iata FROM packet_observations po
      WHERE po.observer_id = o.id
      ORDER BY po.heard_at DESC LIMIT 1
  ) = ANY($1::bpchar[]))
  AND ($2 = '' OR o.observer_type = $2)
  AND ($3 = '' OR ob.broker_name = $3)
  AND ($4 = '' OR CASE
    WHEN GREATEST(COALESCE(o.last_status_at, o.last_seen), o.last_seen) > NOW() - INTERVAL '5 minutes' THEN 'online'
    ELSE 'offline'
  END = $4)
  AND ($5 = '' OR o.display_name ILIKE '%' || $5 || '%')
  AND ($6::timestamptz IS NULL OR o.last_seen < $6)
  AND ($8::text = '' OR EXISTS (
    SELECT 1 FROM observer_scopes os2
    JOIN transport_scopes ts2 ON ts2.id = os2.scope_id
    WHERE os2.observer_id = o.id AND ts2.name = $8::text
  ))
GROUP BY o.id
ORDER BY o.last_seen DESC
LIMIT $7;

-- name: GetObserverLastIATA :one
SELECT iata FROM packet_observations
WHERE observer_id = $1
ORDER BY heard_at DESC
LIMIT 1;

-- name: GetObserverRadio :one
SELECT radio_freq_mhz, radio_bw_khz, radio_sf, radio_cr
FROM observers
WHERE id = $1;

-- name: InsertObserverTelemetry :exec
-- Inserts a telemetry snapshot for an observer. The reported_at timestamp should
-- be truncated to the configured resolution before calling to ensure deduplication.
INSERT INTO observer_telemetry (
    observer_id, reported_at, battery_voltage_mv, airtime_tx_pct,
    airtime_rx_pct, noise_floor_db, uptime_seconds, queue_length,
    debug_flags, receive_errors
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (observer_id, reported_at) DO NOTHING;

-- name: GetObserverTelemetry :many
SELECT id, reported_at, battery_voltage_mv, airtime_tx_pct, airtime_rx_pct,
       noise_floor_db, uptime_seconds, queue_length, debug_flags, receive_errors
FROM observer_telemetry
WHERE observer_id = $1
  AND ($2::timestamptz IS NULL OR reported_at >= $2)
  AND ($3::timestamptz IS NULL OR reported_at <= $3)
  AND ($4 = 0 OR id > $4)
ORDER BY reported_at ASC;

-- name: GetObserverTelemetryBucketed :many
SELECT
  (date_trunc('day', reported_at) +
    (EXTRACT(HOUR FROM reported_at)::int / $4::int) * ($4::int * interval '1 hour'))::timestamptz AS bucket,
  AVG(battery_voltage_mv)::int   AS battery_voltage_mv,
  GREATEST(MAX(airtime_tx_pct) - MIN(airtime_tx_pct), 0)::real AS airtime_tx_pct,
  GREATEST(MAX(airtime_rx_pct) - MIN(airtime_rx_pct), 0)::real AS airtime_rx_pct,
  AVG(noise_floor_db)::real      AS noise_floor_db,
  MAX(uptime_seconds)::bigint    AS uptime_seconds,
  AVG(queue_length)::int         AS queue_length,
  GREATEST(MAX(receive_errors) - MIN(receive_errors), 0)::int  AS receive_errors
FROM observer_telemetry
WHERE observer_id = $1
  AND ($2::timestamptz IS NULL OR reported_at >= $2)
  AND ($3::timestamptz IS NULL OR reported_at <= $3)
GROUP BY bucket
ORDER BY bucket ASC;

-- name: ListObserverAdverts :many
-- Returns advert packets (payload_type=4) heard by a specific observer.
-- Pass cursor=0 to start from the beginning, or the last seen id for pagination.
SELECT 
  po.id,
  encode(po.packet_hash, 'hex') AS packet_hash_hex,
  p.payload_type,
  po.iata,
  po.heard_at,
  po.rssi,
  po.snr,
  po.hop_count,
  n.name AS node_name,
  encode(p.origin_pubkey, 'hex') AS node_public_key
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
LEFT JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE po.observer_id = $1
  AND p.payload_type = 4
  AND ($2 = 0 OR po.id > $2)
ORDER BY po.id ASC
LIMIT $3;

-- name: DeleteOldTelemetry :exec
-- Deletes telemetry rows older than the given cutoff. Called by the cleanup goroutine.
DELETE FROM observer_telemetry WHERE reported_at < $1;

-- ============================================================
-- OBSERVER BROKERS
-- ============================================================

-- name: UpsertObserverBroker :exec
INSERT INTO observer_brokers (observer_id, broker_name, last_seen, last_packet_at)
VALUES ($1, $2, NOW(), NOW())
ON CONFLICT (observer_id, broker_name) DO UPDATE SET
  last_seen      = NOW(),
  last_packet_at = NOW();

-- name: TouchObserverBrokers :exec
UPDATE observer_brokers ob SET
  last_seen      = GREATEST(ob.last_seen, v.seen),
  last_packet_at = GREATEST(ob.last_packet_at, v.seen)
FROM (
  SELECT unnest($1::uuid[]) AS observer_id,
         unnest($2::text[]) AS broker_name,
         unnest($3::timestamptz[]) AS seen
) v
WHERE ob.observer_id = v.observer_id AND ob.broker_name = v.broker_name;

-- ============================================================
-- PACKETS
-- ============================================================

-- name: UpsertPacket :one
INSERT INTO packets (
  packet_hash,
  payload_type,
  payload_version,
  route_type,
  transport_codes_present,
  region_code,
  sub_region_code,
  origin_pubkey,
  raw_payload,
  raw_header,
  parsed_payload,
  channel_hash,
  scope_id,
  trace_tag,
  first_heard_at,
  last_heard_at
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW()
)
ON CONFLICT (packet_hash) DO UPDATE SET
  last_heard_at = NOW()
RETURNING packet_hash, payload_type, payload_version, route_type, transport_codes_present, region_code, sub_region_code, origin_pubkey, raw_payload, raw_header, parsed_payload, decrypted, channel_hash, first_heard_at, last_heard_at, (xmax = 0)
AS inserted;

-- name: SetPacketDecrypted :exec
UPDATE packets SET decrypted = true WHERE packet_hash = $1;

-- name: TouchPackets :exec
UPDATE packets p SET
  last_heard_at = GREATEST(p.last_heard_at, v.heard)
FROM (
  SELECT unnest($1::bytea[]) AS packet_hash,
         unnest($2::timestamptz[]) AS heard
) v
WHERE p.packet_hash = v.packet_hash;

-- name: GetPacketByHash :one
SELECT p.*, ts.name AS scope_name,
    cm.sender_name AS cm_sender_name,
    cm.content AS cm_content,
    cm.sent_at AS cm_sent_at
FROM packets p
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
LEFT JOIN channel_messages cm ON cm.packet_hash = p.packet_hash
WHERE p.packet_hash = $1;

-- name: GetPacketsByTraceTag :many
-- Returns all packets for a given trace tag with observations.
SELECT encode(p.packet_hash, 'hex') AS packet_hash_hex,
    p.route_type,
    p.first_heard_at,
    p.last_heard_at,
    p.parsed_payload,
    p.scope_id,
    ts.name AS scope_name
FROM packets p
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE p.trace_tag = decode($1, 'hex')
ORDER BY p.first_heard_at ASC;

-- name: GetPacketObservationCount :one
SELECT COUNT(*) FROM packet_observations WHERE packet_hash = $1;

-- name: ListPackets :many
-- Returns packets with the latest observation rolled in for display.
-- Pass cursor=0 to start from the beginning. IATA-filtered requests are
-- served by ListPacketsByIATAs instead.
SELECT
  p.packet_hash,
  p.payload_type,
  p.route_type,
  p.first_heard_at,
  p.last_heard_at,
  p.scope_id,
  ts.name AS scope_name,
  (SELECT COUNT(*) FROM packet_observations po2 WHERE po2.packet_hash = p.packet_hash) AS observation_count,
  -- sqlc loses LATERAL nullability; these scalar defaults are ignored when observer_id is NULL.
  po.observer_id AS latest_observer_id,
  o.display_name AS latest_observer_name,
  COALESCE(po.iata, ''::bpchar) AS latest_observer_iata,
  COALESCE(po.path_length_byte, 0::smallint) AS latest_observer_path_length_byte,
  COALESCE(po.hash_size, 0::smallint) AS latest_observer_hash_size,
  COALESCE(po.hop_count, 0::smallint) AS latest_observer_hop_count,
  po.path_bytes AS latest_observer_path_bytes
FROM packets p
LEFT JOIN LATERAL (
  SELECT observer_id, iata, path_length_byte, hash_size, hop_count, path_bytes
  FROM packet_observations
  WHERE packet_hash = p.packet_hash
  ORDER BY heard_at DESC
  LIMIT 1
) po ON true
LEFT JOIN observers o ON o.id = po.observer_id
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE
  (COALESCE(cardinality($1::smallint[]), 0) = 0 OR p.payload_type = ANY($1::smallint[]))
  AND (COALESCE(cardinality($2::smallint[]), 0) = 0 OR p.route_type = ANY($2::smallint[]))
  AND ($3::timestamptz IS NULL OR p.first_heard_at >= $3)
  AND ($4::timestamptz IS NULL OR p.first_heard_at <= $4)
  AND ($5::timestamptz IS NULL OR p.last_heard_at < $5)
  AND (COALESCE(cardinality($7::text[]), 0) = 0 OR ts.name = ANY($7::text[]))
ORDER BY p.last_heard_at DESC
LIMIT $6;

-- name: ListPacketsByIATAs :many
-- IATA-filtered packet list, driven from idx_observations_iata_heard.
-- Walking packets newest-first and probing for the site probes ~589k packets
-- to fill a page for a quiet site; walking the site's own observation log is
-- proportional to the page size instead. Results are ordered by when the
-- requested sites heard the packet (site-local recency) and the cursor
-- follows that ordering. scan_depth caps how deep each site's observation
-- log is walked. A packet repeats once per observer that heard it, so a
-- page can collapse to fewer distinct packets than were asked for without
-- the site being exhausted. scan_saturated reports whether any site hit
-- that cap and scan_floor the oldest heard_at they all cover, so a short
-- page can keep paging instead of reading as the end of the data.
WITH scanned AS (
  SELECT req.iata AS req_iata, hits.packet_hash, hits.heard_at
  FROM unnest(@iatas::bpchar[]) AS req(iata)
  CROSS JOIN LATERAL (
    SELECT po3.packet_hash, po3.heard_at
    FROM packet_observations po3
    JOIN packets p2 ON p2.packet_hash = po3.packet_hash
    WHERE po3.iata = req.iata
      AND po3.heard_at < COALESCE(@cursor_ts::timestamptz, 'infinity'::timestamptz)
      AND (COALESCE(cardinality(@payload_types::smallint[]), 0) = 0 OR p2.payload_type = ANY(@payload_types::smallint[]))
      AND (COALESCE(cardinality(@route_types::smallint[]), 0) = 0 OR p2.route_type = ANY(@route_types::smallint[]))
      AND (@since_ts::timestamptz IS NULL OR p2.first_heard_at >= @since_ts)
      AND (@until_ts::timestamptz IS NULL OR p2.first_heard_at <= @until_ts)
      AND (COALESCE(cardinality(@scope_names::text[]), 0) = 0 OR EXISTS (
        SELECT 1 FROM transport_scopes ts2
        WHERE ts2.id = p2.scope_id AND ts2.name = ANY(@scope_names::text[])))
    ORDER BY po3.heard_at DESC
    LIMIT @scan_depth
  ) hits
),
-- A site that filled scan_depth still has unread history below its floor.
-- The newest such floor is the point above which every site is covered.
saturation AS (
  SELECT
    COUNT(*) > 0 AS scan_saturated,
    MAX(floor_ts)::timestamptz AS scan_floor
  FROM (
    SELECT MIN(heard_at) AS floor_ts
    FROM scanned
    GROUP BY req_iata
    HAVING COUNT(*) >= @scan_depth
  ) filled
),
page AS (
  SELECT scanned.packet_hash, MAX(scanned.heard_at)::timestamptz AS site_heard_at
  FROM scanned
  GROUP BY scanned.packet_hash
  HAVING (@cursor_ts::timestamptz IS NULL OR NOT EXISTS (
    SELECT 1 FROM packet_observations px
    WHERE px.packet_hash = scanned.packet_hash
      AND px.iata = ANY(@iatas::bpchar[])
      AND px.heard_at >= @cursor_ts))
  ORDER BY site_heard_at DESC
  LIMIT @page_limit
)
SELECT
  p.packet_hash,
  p.payload_type,
  p.route_type,
  p.first_heard_at,
  p.last_heard_at,
  p.scope_id,
  ts.name AS scope_name,
  sh.site_heard_at,
  sat.scan_saturated,
  sat.scan_floor,
  (SELECT COUNT(*) FROM packet_observations po2 WHERE po2.packet_hash = p.packet_hash) AS observation_count,
  -- sqlc loses LATERAL nullability; these scalar defaults are ignored when observer_id is NULL.
  po.observer_id AS latest_observer_id,
  o.display_name AS latest_observer_name,
  COALESCE(po.iata, ''::bpchar) AS latest_observer_iata,
  COALESCE(po.path_length_byte, 0::smallint) AS latest_observer_path_length_byte,
  COALESCE(po.hash_size, 0::smallint) AS latest_observer_hash_size,
  COALESCE(po.hop_count, 0::smallint) AS latest_observer_hop_count,
  po.path_bytes AS latest_observer_path_bytes
FROM page sh
CROSS JOIN saturation sat
JOIN packets p ON p.packet_hash = sh.packet_hash
LEFT JOIN LATERAL (
  SELECT observer_id, iata, path_length_byte, hash_size, hop_count, path_bytes
  FROM packet_observations
  WHERE packet_hash = p.packet_hash
  ORDER BY heard_at DESC
  LIMIT 1
) po ON true
LEFT JOIN observers o ON o.id = po.observer_id
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
ORDER BY sh.site_heard_at DESC;

-- name: ListPacketsAfterID :many
-- Returns packets with observations after the given observation ID, ordered oldest first.
-- Used for WS reconnect backfill. Pass afterObservationId=0 to start from the beginning.
SELECT
  p.packet_hash,
  p.payload_type,
  p.route_type,
  p.first_heard_at,
  p.last_heard_at,
  (SELECT COUNT(*) FROM packet_observations po2 WHERE po2.packet_hash = p.packet_hash) AS observation_count,
  po.observer_id AS latest_observer_id,
  o.display_name AS latest_observer_name,
  po.iata AS latest_observer_iata,
  po.path_length_byte AS latest_observer_path_length_byte,
  po.hash_size AS latest_observer_hash_size,
  po.hop_count AS latest_observer_hop_count,
  po.path_bytes AS latest_observer_path_bytes,
  ts.name AS scope_name
FROM packets p
JOIN packet_observations po ON po.packet_hash = p.packet_hash
LEFT JOIN observers o ON o.id = po.observer_id
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE po.id > $1
  AND ($2::smallint = -1 OR p.payload_type = $2::smallint)
  AND ($3::smallint = -1 OR p.route_type = $3::smallint)
  AND (COALESCE(cardinality($4::bpchar[]), 0) = 0 OR po.iata = ANY($4::bpchar[]))
  AND ($5::text = '' OR ts.name = $5::text)
ORDER BY po.id ASC
LIMIT $6;


-- name: DeleteOldPackets :exec
-- Deletes packets and their observations older than the given cutoff.
-- packet_observations cascade-delete via FK.
DELETE FROM packets WHERE last_heard_at < $1;

-- name: DeleteOldNodes :exec
-- Deletes nodes not seen since the given cutoff. node_iatas and node_neighbors cascade-
-- delete via FK. Excludes nodes referenced by observer_owners.owner_node_id -- that FK has
-- no ON DELETE action, so deleting one directly would fail the whole statement anyway, and
-- an operator manually recorded ownership for that node, so leave it alone even if stale.
-- known_routes.node_ids is a plain UUID[] with no FK; a deleted node's id can be left
-- dangling in old routes there, but ReconfirmTask already prunes stale/ambiguous routes
-- periodically and will clean those up on its own schedule.
DELETE FROM nodes
WHERE last_seen < $1
  AND id NOT IN (SELECT owner_node_id FROM observer_owners WHERE owner_node_id IS NOT NULL);

-- name: DeleteOldRoutes :exec
-- Deletes routes not observed since the retention cutoff ($1), and rarely-observed
-- routes (observation_count < $2) not observed since the grace cutoff ($3).
DELETE FROM known_routes
WHERE last_seen < $1
   OR (observation_count < $2 AND last_seen < $3);

-- name: DeleteOldChannelIATAs :exec
-- Keeps the channel IATA filter in step with packet retention.
DELETE FROM channel_iatas WHERE last_heard < $1;

-- name: DeleteOldTraceIATAs :exec
-- Keeps the trace IATA filter in step with packet retention.
DELETE FROM trace_iatas WHERE last_heard < $1;

-- ============================================================
-- PACKET OBSERVATIONS
-- ============================================================

-- name: InsertObservation :one
INSERT INTO packet_observations (
  packet_hash,
  observer_id,
  iata,
  heard_at,
  path_length_byte,
  hash_size,
  hop_count,
  path_bytes,
  rssi,
  snr,
  propagation_time_ms,
  radio_freq_mhz,
  spread_factor,
  bandwidth_khz,
  coding_rate,
  source_broker,
  payload_type
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
)
ON CONFLICT (packet_hash, observer_id) DO NOTHING
RETURNING *;

-- name: ListObservationsForPacket :many
SELECT po.*, o.display_name AS observer_name
FROM packet_observations po
LEFT JOIN observers o ON o.id = po.observer_id
WHERE po.packet_hash = $1
ORDER BY po.heard_at ASC;

-- ============================================================
-- NODES
-- ============================================================

-- name: UpsertNode :one
INSERT INTO nodes (public_key, node_type, name, latitude, longitude, location_source, last_advert_at, last_seen, radio_freq_mhz, radio_sf, radio_bw_khz, device_clock_drift_seconds)
VALUES ($1, $2, $3, $4, $5, 'advert', NOW(), NOW(), $6, $7, $8, $9)
ON CONFLICT (public_key) DO UPDATE SET
  node_type       = EXCLUDED.node_type,
  name            = COALESCE(EXCLUDED.name, nodes.name),
  latitude        = COALESCE(EXCLUDED.latitude, nodes.latitude),
  longitude       = COALESCE(EXCLUDED.longitude, nodes.longitude),
  location_source = CASE WHEN EXCLUDED.latitude IS NOT NULL THEN 'advert' ELSE nodes.location_source END,
  last_advert_at  = NOW(),
  last_seen       = NOW(),
  radio_freq_mhz  = EXCLUDED.radio_freq_mhz,
  radio_sf        = EXCLUDED.radio_sf,
  radio_bw_khz    = EXCLUDED.radio_bw_khz,
  device_clock_drift_seconds = EXCLUDED.device_clock_drift_seconds
RETURNING *;

-- name: SetNodeMultibytePaths :exec
UPDATE nodes SET supports_multibyte_paths = TRUE
WHERE id = $1 AND supports_multibyte_paths = FALSE;

-- name: SetNodeMultibyteTraces :exec
UPDATE nodes SET supports_multibyte_traces = TRUE
WHERE id = $1 AND supports_multibyte_traces = FALSE;

-- name: SetNodeDefaultScope :exec
UPDATE nodes SET default_scope_id = $2 WHERE id = $1;

-- name: GetNodeByID :one
SELECT n.*, ts.name AS default_scope_name,
  EXISTS (SELECT 1 FROM observers o WHERE o.public_key = n.public_key) AS is_observer,
  (SELECT o.id FROM observers o WHERE o.public_key = n.public_key LIMIT 1) AS observer_id,
  (SELECT json_agg(json_build_object('iata', ni.iata, 'lastHeard', (extract(epoch from ni.last_heard) * 1000)::bigint) ORDER BY ni.last_heard DESC)
   FROM node_iatas ni WHERE ni.node_id = n.id) AS iatas,
  (SELECT COUNT(DISTINCT nn.neighbor_id) FROM node_neighbors nn WHERE nn.node_id = n.id)::bigint AS known_neighbor_count
FROM nodes n
LEFT JOIN transport_scopes ts ON ts.id = n.default_scope_id
WHERE n.id = $1;

-- name: GetNodesByIDs :many
SELECT id, public_key, name, latitude, longitude
FROM nodes
WHERE id = ANY($1::uuid[]);

-- name: GetNodeByPubkey :one
SELECT id FROM nodes WHERE public_key = $1;

-- name: ListNodes :many
-- Limit the filtered node page before enriching IATA membership and neighbours.
WITH page AS (
SELECT n.id, n.public_key, n.node_type, n.name, n.latitude, n.longitude, n.last_seen,
  n.radio_freq_mhz, n.radio_sf, n.radio_bw_khz, ts.name AS default_scope_name
FROM nodes n
LEFT JOIN transport_scopes ts ON ts.id = n.default_scope_id
WHERE
  ($1 = 0 OR n.node_type = $1)
  AND (COALESCE(cardinality($2::bpchar[]), 0) = 0 OR n.id IN (SELECT node_id FROM node_iatas WHERE iata = ANY($2::bpchar[])))
  AND (
    $3::text = 'any'
    OR ($3::text = 'true' AND n.supports_multibyte_paths = TRUE)
    OR ($3::text = 'false' AND n.supports_multibyte_paths = FALSE)
  )
  AND (
    $4::text = 'any'
    OR ($4::text = 'true' AND n.supports_multibyte_traces = TRUE)
    OR ($4::text = 'false' AND n.supports_multibyte_traces = FALSE)
  )
  AND ($5::bytea IS NULL OR n.public_key = $5)
  AND ($6 = '' OR n.name ILIKE '%' || $6 || '%')
  AND ($7::timestamptz IS NULL OR n.last_seen < $7)
  AND ($9::text = '' OR ts.name = $9::text)
  AND ($11::text = '' OR encode(n.public_key, 'hex') ILIKE $11 || '%')
ORDER BY n.last_seen DESC
LIMIT $8
)
SELECT n.id, n.public_key, n.node_type, n.name, n.latitude, n.longitude, n.last_seen,
  n.radio_freq_mhz, n.radio_sf, n.radio_bw_khz, n.default_scope_name,
  (SELECT json_agg(json_build_object('iata', ni.iata, 'lastHeard', (extract(epoch from ni.last_heard) * 1000)::bigint) ORDER BY ni.last_heard DESC)
   FROM node_iatas ni WHERE ni.node_id = n.id) AS iatas,
  EXISTS (SELECT 1 FROM observers o WHERE o.public_key = n.public_key) AS is_observer,
  (SELECT o.id FROM observers o WHERE o.public_key = n.public_key LIMIT 1) AS observer_id,
  (SELECT COUNT(DISTINCT nn.neighbor_id) FROM node_neighbors nn WHERE nn.node_id = n.id)::bigint AS known_neighbor_count,
  (CASE WHEN $10::bool THEN
    (SELECT COALESCE(array_agg(DISTINCT nn.neighbor_id), '{}'::uuid[]) FROM node_neighbors nn WHERE nn.node_id = n.id)
  ELSE NULL END)::uuid[] AS neighbor_ids
FROM page n
ORDER BY n.last_seen DESC;

-- name: ListNodeObservations :many
SELECT po.id, encode(po.packet_hash, 'hex') AS packet_hash_hex,
  p.payload_type, po.iata, po.heard_at, po.rssi, po.snr, po.hop_count
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE n.id = $1
  AND ($2 = 0 OR po.id < $2)
ORDER BY po.id DESC
LIMIT $3;

-- ============================================================
-- NODE IATAS
-- ============================================================

-- name: UpsertNodeIATA :exec
INSERT INTO node_iatas (node_id, iata, last_heard, observation_count)
VALUES ($1, $2, NOW(), 1)
ON CONFLICT (node_id, iata) DO UPDATE SET
  last_heard        = NOW(),
  observation_count = node_iatas.observation_count + 1;

-- name: UpsertNodeShortID :exec
INSERT INTO node_short_ids (node_id, iata, prefix_4)
VALUES ($1, $2, $3)
ON CONFLICT (node_id, iata) DO NOTHING;

-- ============================================================
-- CHANNELS
-- ============================================================

-- name: UpsertChannel :one
-- Upsert a channel by (hash, key_fingerprint). Pass NULL fingerprint for
-- hash-only records (key unknown). Returns the channel row.
INSERT INTO channels (channel_hash, key_fingerprint, name, hashtag, is_hashtag, key_known, last_seen)
VALUES ($1, $2::bytea, $3, $4, $5, ($2 IS NOT NULL), NOW())
ON CONFLICT (channel_hash, key_fingerprint) DO UPDATE SET
  last_seen     = NOW(),
  name          = COALESCE(EXCLUDED.name, channels.name),
  message_count = CASE WHEN $6 THEN channels.message_count + 1 ELSE channels.message_count END
RETURNING *;

-- name: UpsertChannelHashOnly :one
INSERT INTO channels (channel_hash, last_seen)
VALUES ($1, NOW())
ON CONFLICT (channel_hash) WHERE key_fingerprint IS NULL DO UPDATE SET
  last_seen = NOW()
RETURNING id;

-- name: ListUndecryptedGroupTextPackets :many
-- Returns GRP_TXT packets (payload_type=5) never successfully decrypted. Used at boot to
-- retry decryption against the current keystore for packets whose channel key was only added
-- to the config after they'd already been ingested -- see
-- internal/ingest.BackfillChannelMessages.
SELECT packet_hash, raw_payload FROM packets
WHERE payload_type = 5 AND decrypted IS NOT TRUE;

-- name: UpsertChannelIATA :exec
-- Refreshes at most hourly so repeat hears don't churn the row.
INSERT INTO channel_iatas (channel_hash, iata, last_heard)
VALUES ($1, $2, $3)
ON CONFLICT (channel_hash, iata) DO UPDATE SET
  last_heard = EXCLUDED.last_heard
WHERE EXCLUDED.last_heard > channel_iatas.last_heard + INTERVAL '1 hour';

-- name: UpsertTraceIATA :exec
-- Refreshes at most hourly so repeat hears don't churn the row.
INSERT INTO trace_iatas (trace_tag, iata, last_heard)
VALUES ($1, $2, $3)
ON CONFLICT (trace_tag, iata) DO UPDATE SET
  last_heard = EXCLUDED.last_heard
WHERE EXCLUDED.last_heard > trace_iatas.last_heard + INTERVAL '1 hour';

-- name: ListChannels :many
-- Channels ordered by last seen, optionally filtered by hash and/or IATAs
-- (membership via channel_iatas). NULL hash / empty array skip those filters.
-- Pass cursor=0 to start from the beginning (cursor is last_seen epoch ms).
SELECT c.* FROM channels c
WHERE (@channel_hash::bytea IS NULL OR c.channel_hash = @channel_hash)
  AND (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0 OR c.channel_hash IN (
    SELECT ci.channel_hash FROM channel_iatas ci
    WHERE ci.iata = ANY(@iatas::bpchar[])
  ))
  AND (@cursor_ts::timestamptz IS NULL OR c.last_seen < @cursor_ts)
ORDER BY c.last_seen DESC
LIMIT @page_limit;

-- name: GetChannelByID :one
SELECT * FROM channels WHERE id = $1;


-- ============================================================
-- CHANNEL MESSAGES
-- ============================================================

-- name: InsertChannelMessage :one
INSERT INTO channel_messages (channel_id, packet_hash, sender_name, content, sent_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (packet_hash) DO NOTHING
RETURNING id;

-- name: ListChannelMessages :many
-- Returns messages for a channel identified by integer ID.
-- Pass a zero/null timestamp for since to return all messages up to limit.
-- Pass empty string for iata to skip IATA filtering.
-- Pass cursor=0 to start from the beginning.
SELECT DISTINCT ON (cm.id) cm.*, encode(cm.packet_hash, 'hex') as packet_hash_hex, c.channel_hash,
(SELECT COUNT(*) FROM packet_observations po2 WHERE po2.packet_hash = cm.packet_hash) AS observation_count
FROM channel_messages cm
JOIN channels c ON c.id = cm.channel_id
JOIN packet_observations po ON po.packet_hash = cm.packet_hash
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE cm.channel_id = $1
  AND ($2::timestamptz IS NULL OR cm.sent_at >= $2)
  AND (COALESCE(cardinality($3::bpchar[]), 0) = 0 OR po.iata = ANY($3::bpchar[]))
  AND ($4::text = '' OR ts.name = $4::text)
  AND ($5::bigint = 0 OR cm.id < $5::bigint)
ORDER BY cm.id DESC
LIMIT $6;

-- name: ListAllChannelMessages :many
-- Returns all messages across all channels with optional time, IATA, scope and cursor filters.
-- Pass empty string for iata or scope to skip those filters.
-- Pass cursor=0 to start from the beginning.
SELECT DISTINCT ON (cm.id) cm.*, encode(cm.packet_hash, 'hex') as packet_hash_hex, c.channel_hash,
(SELECT COUNT(*) FROM packet_observations po2 WHERE po2.packet_hash = cm.packet_hash) AS observation_count
FROM channel_messages cm
JOIN channels c ON c.id = cm.channel_id
JOIN packet_observations po ON po.packet_hash = cm.packet_hash
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE ($1::timestamptz IS NULL OR cm.sent_at >= $1)
  AND (COALESCE(cardinality($2::bpchar[]), 0) = 0 OR po.iata = ANY($2::bpchar[]))
  AND ($3::text = '' OR ts.name = $3::text)
  AND ($4 = 0 OR cm.id < $4)
ORDER BY cm.id DESC
LIMIT $5;

-- name: ListChannelMessagesByHash :many
-- Returns messages for all channels matching a hash byte.
-- May return messages from multiple channels if the hash collides across different keys.
-- Pass empty string for iata or scope to skip those filters.
-- Pass cursor=0 to start from the beginning.
SELECT DISTINCT ON (cm.id) cm.*, c.channel_hash,
  (SELECT COUNT(*) FROM packet_observations po2 WHERE po2.packet_hash = cm.packet_hash) AS observation_count
FROM channel_messages cm
JOIN channels c ON c.id = cm.channel_id
JOIN packet_observations po ON po.packet_hash = cm.packet_hash
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE c.channel_hash = $1
  AND ($2::timestamptz IS NULL OR cm.sent_at >= $2)
  AND (COALESCE(cardinality($3::bpchar[]), 0) = 0 OR po.iata = ANY($3::bpchar[]))
  AND ($4::text = '' OR ts.name = $4::text)
  AND ($5::bigint = 0 OR cm.id < $5::bigint)
ORDER BY cm.id DESC
LIMIT $6;

-- name: ListMessagesAfterID :many
-- Returns messages after the given message ID, ordered oldest first.
-- Used for WS reconnect backfill.
SELECT DISTINCT ON (cm.id) cm.*, encode(cm.packet_hash, 'hex') as packet_hash_hex, c.channel_hash,
(SELECT COUNT(*) FROM packet_observations po2 WHERE po2.packet_hash = cm.packet_hash) AS observation_count
FROM channel_messages cm
JOIN channels c ON c.id = cm.channel_id
JOIN packet_observations po ON po.packet_hash = cm.packet_hash
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE cm.id > $1
  AND (COALESCE(cardinality($2::bpchar[]), 0) = 0 OR po.iata = ANY($2::bpchar[]))
  AND ($3::text = '' OR ts.name = $3::text)
ORDER BY cm.id ASC
LIMIT $4;

-- ============================================================
-- STATS
-- ============================================================

-- name: GetStatsOverview :one
SELECT
  COUNT(DISTINCT po.packet_hash)  AS total_packets,
  COUNT(*)                        AS total_observations,
  COUNT(DISTINCT po.observer_id)  AS active_observers,
  COUNT(DISTINCT po.iata)         AS active_iatas
FROM packet_observations po
WHERE po.heard_at > NOW() - INTERVAL '24 hours'
  AND (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR po.iata = ANY($1::bpchar[]));

-- name: GetHourlyStats :many
SELECT iata, hour, observation_count, unique_packets, active_observers
FROM mv_hourly_iata_stats
WHERE (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR iata = ANY($1::bpchar[]))
  AND hour >= NOW() - $2::interval
ORDER BY iata, hour;

-- name: GetTopNodes :many
SELECT * FROM mv_top_nodes_by_iata
WHERE (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR iata = ANY($1::bpchar[]))
ORDER BY observation_count DESC
LIMIT $2;

-- name: GetStatsPayloadBreakdown :many
-- Payload-type counts for the IATA within the window, summed from the
-- precomputed hourly buckets.
SELECT
  payload_type,
  SUM(count)::bigint AS count
FROM mv_payload_breakdown_by_iata
WHERE (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR iata = ANY($1::bpchar[]))
  AND bucket >= NOW() - $2::interval
GROUP BY payload_type
ORDER BY count DESC;

-- name: GetStatsNodeTypes :many
-- Returns node counts grouped by type, optionally filtered by IATA.
SELECT
  n.node_type,
  COUNT(DISTINCT n.id)::bigint AS count
FROM nodes n
LEFT JOIN node_iatas ni ON ni.node_id = n.id
WHERE (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR ni.iata = ANY($1::bpchar[]))
GROUP BY n.node_type
ORDER BY count DESC;

-- name: GetStatsTopObservers :many
-- Top N observers for the IATA within the window, summed from the precomputed
-- hourly buckets. Counts sum across matched IATAs; iata is a representative one.
SELECT
  observer_id AS id,
  display_name,
  observer_type,
  COALESCE(SUM(observation_count), 0)::bigint AS observation_count,
  COALESCE(MAX(iata), '')::bpchar AS iata
FROM mv_top_observers_by_iata
WHERE bucket >= NOW() - $1::interval
  AND (COALESCE(cardinality($2::bpchar[]), 0) = 0 OR iata = ANY($2::bpchar[]))
GROUP BY observer_id, display_name, observer_type
ORDER BY observation_count DESC
LIMIT $3;

-- name: GetStatsTopAdvertisers :many
-- Top N advertisers in the window, summed from the hourly buckets.
SELECT
  node_id AS id,
  name,
  node_type,
  COALESCE(SUM(advert_count), 0)::bigint AS advert_count,
  COALESCE(SUM(flood_advert_count), 0)::bigint AS flood_advert_count,
  COALESCE(SUM(direct_advert_count), 0)::bigint AS direct_advert_count,
  MAX(last_heard)::timestamptz AS last_heard,
  COALESCE(MAX(iata), '')::bpchar AS iata
FROM mv_top_advertisers_by_iata
WHERE bucket >= NOW() - $1::interval
  AND (COALESCE(cardinality($2::bpchar[]), 0) = 0 OR iata = ANY($2::bpchar[]))
GROUP BY node_id, name, node_type
ORDER BY advert_count DESC
LIMIT $3;

-- name: GetStatsClockDrift :many
-- Repeaters/room servers (node_type 2/3) whose current advert-derived clock drift exceeds
-- the given threshold in magnitude, worst first. Not time-windowed -- reflects each node's
-- latest measured drift, not an aggregate over a period.
SELECT
  n.id,
  n.name,
  n.node_type,
  n.device_clock_drift_seconds,
  n.last_advert_at,
  json_agg(json_build_object('iata', ni.iata, 'lastHeard', (extract(epoch from ni.last_heard) * 1000)::bigint) ORDER BY ni.last_heard DESC) FILTER (WHERE ni.iata IS NOT NULL) AS iatas
FROM nodes n
LEFT JOIN node_iatas ni ON ni.node_id = n.id
WHERE n.node_type IN (2, 3)
  AND n.device_clock_drift_seconds IS NOT NULL
  AND ABS(n.device_clock_drift_seconds) > $1::int
  AND (COALESCE(cardinality($2::bpchar[]), 0) = 0 OR n.id IN (SELECT node_id FROM node_iatas WHERE iata = ANY($2::bpchar[])))
GROUP BY n.id, n.name, n.node_type, n.device_clock_drift_seconds, n.last_advert_at
ORDER BY ABS(n.device_clock_drift_seconds) DESC
LIMIT $3;

-- name: GetStatsTopTalkers :many
-- Top N talkers (by decrypted sender_name) in the window, summed from the hourly buckets.
SELECT
  sender_name,
  COALESCE(SUM(message_count), 0)::bigint AS message_count,
  MAX(last_sent)::timestamptz AS last_sent
FROM mv_top_talkers_by_iata
WHERE bucket >= NOW() - $1::interval
  AND (COALESCE(cardinality($2::bpchar[]), 0) = 0 OR iata = ANY($2::bpchar[]))
GROUP BY sender_name
ORDER BY message_count DESC
LIMIT $3;

-- name: GetRadioPresets :many
SELECT preset, iata, source_type, count
FROM mv_radio_presets
WHERE ($1::text = '' OR preset = $1::text)
  AND (COALESCE(cardinality($2::bpchar[]), 0) = 0 OR iata = ANY($2::bpchar[]))
ORDER BY preset, iata, source_type;

-- name: GetScopeStats :many
-- Count each table on its own; the old cross-join blew up to millions of rows
-- before COUNT(DISTINCT) (~10s).
SELECT
    ts.name,
    (SELECT COUNT(*) FROM packets p WHERE p.scope_id = ts.id) AS packet_count,
    (SELECT COUNT(*) FROM observer_scopes os WHERE os.scope_id = ts.id) AS observer_count,
    (SELECT COUNT(*) FROM nodes n WHERE n.default_scope_id = ts.id) AS node_count
FROM transport_scopes ts
ORDER BY ts.name;

-- ============================================================
-- REGIONS
-- ============================================================

-- name: ListRegions :many
SELECT id, slug, name
FROM regions
ORDER BY display_order, name;

-- name: GetRegion :one
SELECT id, slug, name, description, center_lat, center_lng, zoom_level
FROM regions
WHERE id = $1;

-- name: GetRegionBySlug :one
SELECT id, slug, name, description, center_lat, center_lng, zoom_level
FROM regions
WHERE slug = $1;

-- name: GetRegionIATAs :many
SELECT iata FROM region_iatas
WHERE region_id = $1
ORDER BY iata;

-- name: UpsertRegion :one
INSERT INTO regions (slug, name, description, display_order, center_lat, center_lng, zoom_level, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
ON CONFLICT (slug) DO UPDATE SET
    name          = EXCLUDED.name,
    description   = EXCLUDED.description,
    display_order = EXCLUDED.display_order,
    center_lat    = EXCLUDED.center_lat,
    center_lng    = EXCLUDED.center_lng,
    zoom_level    = EXCLUDED.zoom_level,
    updated_at    = NOW()
RETURNING id;

-- name: UpsertRegionIATA :exec
INSERT INTO region_iatas (region_id, iata)
VALUES ($1, $2)
ON CONFLICT (region_id, iata) DO NOTHING;

-- ============================================================
-- TRACES
-- ============================================================

-- name: ListTraceTags :many
-- Returns distinct trace tags with summary info, ordered by most recent first.
-- IATA membership comes from trace_iatas (joining observations here spilled the
-- hash join). Per-tag details filled in only for the returned page.
WITH tags AS (
    SELECT
        p.trace_tag,
        MIN(p.first_heard_at) AS first_heard_at,
        MAX(p.last_heard_at) AS last_heard_at,
        COUNT(*) AS packet_count,
        MAX(p.parsed_payload->>'type') AS trace_type
    FROM packets p
    WHERE p.trace_tag IS NOT NULL
      AND (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR p.trace_tag IN (
          SELECT ti.trace_tag FROM trace_iatas ti WHERE ti.iata = ANY($1::bpchar[])))
      AND ($2::text = '' OR p.scope_id = (SELECT id FROM transport_scopes WHERE name = $2))
      AND ($3::timestamptz IS NULL OR p.first_heard_at >= $3)
      AND ($4::timestamptz IS NULL OR p.first_heard_at <= $4)
      AND ($5::timestamptz IS NULL OR p.last_heard_at < $5)
      AND ($7::text = '' OR p.parsed_payload->>'type' = $7)
    GROUP BY p.trace_tag
    ORDER BY MAX(p.last_heard_at) DESC
    LIMIT $6
)
SELECT
    encode(t.trace_tag, 'hex') AS trace_tag,
    t.first_heard_at::timestamptz AS first_heard_at,
    t.last_heard_at::timestamptz AS last_heard_at,
    t.packet_count,
    (SELECT COUNT(*)
     FROM trace_iatas ti
     WHERE ti.trace_tag = t.trace_tag
       AND (COALESCE(cardinality($1::bpchar[]), 0) = 0 OR ti.iata = ANY($1::bpchar[]))) AS iata_count,
    t.trace_type::text AS trace_type,
    (SELECT p3.parsed_payload
     FROM packets p3
     WHERE p3.trace_tag = t.trace_tag
     ORDER BY jsonb_array_length(p3.parsed_payload->'pathHashes') DESC
     LIMIT 1) AS best_payload
FROM tags t
ORDER BY t.last_heard_at DESC;

-- ============================================================
-- ROUTES
-- ============================================================

-- name: UpsertKnownRoute :exec
-- Route identity is path_key, an md5 of node_ids computed by the caller.
-- On conflict, observation_count and last_seen are bumped.
INSERT INTO known_routes (path_key, node_ids, hash_prefix, iata, hop_count)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (iata, path_key) DO UPDATE SET
  last_seen = NOW(),
  observation_count = known_routes.observation_count + 1;

-- name: ListKnownRoutes :many
-- Only one branch runs. Keep the IATA range ordered by the composite index:
-- generic plans can otherwise prefer scanning the global timestamp index.
-- The text equality preserves exact input matching, including trailing spaces.
(
SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
FROM known_routes
WHERE $1 = ''
  AND ($2 = 0 OR hop_count = $2)
  AND ($3::timestamptz IS NULL OR last_seen < $3)
ORDER BY last_seen DESC
LIMIT $4
)
UNION ALL
(
SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
FROM known_routes
WHERE $1 <> '' AND iata::text = $1
  AND iata >= $1::bpchar AND iata <= $1::bpchar
  AND ($2 = 0 OR hop_count = $2)
  AND ($3::timestamptz IS NULL OR last_seen < $3)
ORDER BY iata, last_seen DESC
LIMIT $4
)
ORDER BY last_seen DESC
LIMIT $4;

-- name: SearchKnownRoutes :many
-- Returns known routes containing a subsequence from source to destination hash prefix.
-- Verifies source appears before destination in the route.
SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
FROM known_routes
WHERE iata = $1
  AND array_position(hash_prefix, $2::bytea) IS NOT NULL
  AND array_position(hash_prefix, $3::bytea) IS NOT NULL
  AND array_position(hash_prefix, $2::bytea) < array_position(hash_prefix, $3::bytea)
ORDER BY hop_count ASC, last_seen DESC;

-- name: GetKnownRoutesByNode :many
SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
FROM known_routes
WHERE iata = $1
  AND $2::uuid = ANY(node_ids)
ORDER BY hop_count ASC, last_seen DESC;

-- ============================================================
-- NEIGHBORS
-- ============================================================

-- name: UpsertNodeNeighbor :exec
-- Records or updates a neighbor relationship between two nodes observed in the same IATA.
-- node_id is the advertising node, neighbor_id is the first-hop forwarder.
-- snr is optional; pass NULL when no signal reading is available (the
-- common case). regionScope is optional too; pass NULL whenever the OTA
-- scope query for this neighbor didn't succeed (status != "responded"),
-- so a failed/timed-out query doesn't erase a previously known scope.
-- On conflict, snr and region_scope are only overwritten when a new
-- non-null value is supplied.
INSERT INTO node_neighbors (node_id, neighbor_id, iata, observation_count, snr, region_scope)
VALUES ($1, $2, $3, 1, $4, $5)
ON CONFLICT (node_id, neighbor_id, iata) DO UPDATE SET
  last_seen         = NOW(),
  observation_count = node_neighbors.observation_count + 1,
  snr               = COALESCE(EXCLUDED.snr, node_neighbors.snr),
  region_scope      = COALESCE(EXCLUDED.region_scope, node_neighbors.region_scope);

-- name: UpdateObserverRegionScope :exec
-- Records the observer's own OTA-reported region scope, from the "self"
-- field of a /neighbors report. Always known (not queried OTA), so this
-- unconditionally overwrites, unlike the neighbor-side region_scope.
UPDATE observers SET region_scope = $2 WHERE id = $1;

-- name: GetNodeNeighbors :many
-- Returns the neighbors of a node with details, ordered by most recently seen.
SELECT
    n.id, n.public_key, n.name, n.node_type, n.latitude, n.longitude,
    nn.iata, nn.observation_count, nn.first_seen, nn.last_seen, nn.snr
FROM node_neighbors nn
JOIN nodes n ON n.id = nn.neighbor_id
WHERE nn.node_id = $1
ORDER BY nn.last_seen DESC;

-- name: GetCrossIATANeighbors :many
-- Returns neighbors of a node that are in a different IATA.
SELECT
    n.id, n.name, n.node_type, n.latitude, n.longitude,
    nn.iata AS neighbor_iata, nn.observation_count, nn.last_seen, nn.snr
FROM node_neighbors nn
JOIN nodes n ON n.id = nn.neighbor_id
WHERE nn.node_id = $1
  AND nn.iata != $2
ORDER BY nn.last_seen DESC;

-- ============================================================
-- HELPERS
-- ============================================================

-- Path hash resolution is split per prefix width so each query gets a
-- cacheable generic plan on its (iata, prefix_N) index; a single CASE
-- predicate forced a fresh custom plan on every call.

-- name: ResolvePathHashesP1 :many
SELECT ns.prefix_4 AS hash, n.id AS node_id, n.name, n.latitude, n.longitude, n.public_key
FROM node_short_ids ns
JOIN nodes n ON n.id = ns.node_id
WHERE ns.iata = $1
  AND n.node_type IN (2, 3)
  AND ns.prefix_1 = ANY($2::bytea[]);

-- name: ResolvePathHashesP2 :many
SELECT ns.prefix_4 AS hash, n.id AS node_id, n.name, n.latitude, n.longitude, n.public_key
FROM node_short_ids ns
JOIN nodes n ON n.id = ns.node_id
WHERE ns.iata = $1
  AND n.node_type IN (2, 3)
  AND ns.prefix_2 = ANY($2::bytea[]);

-- name: ResolvePathHashesP3 :many
SELECT ns.prefix_4 AS hash, n.id AS node_id, n.name, n.latitude, n.longitude, n.public_key
FROM node_short_ids ns
JOIN nodes n ON n.id = ns.node_id
WHERE ns.iata = $1
  AND n.node_type IN (2, 3)
  AND ns.prefix_3 = ANY($2::bytea[]);

-- name: ResolvePathHashesP4 :many
SELECT ns.prefix_4 AS hash, n.id AS node_id, n.name, n.latitude, n.longitude, n.public_key
FROM node_short_ids ns
JOIN nodes n ON n.id = ns.node_id
WHERE ns.iata = $1
  AND n.node_type IN (2, 3)
  AND ns.prefix_4 = ANY($2::bytea[]);

-- name: RefreshHourlyStats :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_hourly_iata_stats;

-- name: RefreshTopNodes :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_top_nodes_by_iata;

-- name: RefreshTopObservers :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_top_observers_by_iata;

-- name: RefreshPayloadBreakdown :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_payload_breakdown_by_iata;

-- name: RefreshTopTalkers :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_top_talkers_by_iata;

-- name: RefreshTopAdvertisers :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_top_advertisers_by_iata;

-- name: RefreshRadioPresets :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_radio_presets;

-- name: ReconfirmRoutes :exec
-- Checks the $1 least-recently-reconfirmed routes: deletes those with a departed
-- hop node or a hop prefix now matching >1 node in that IATA (length-aware:
-- 1/2/3/4-byte hop prefixes check prefix_1/2/3/4), and stamps the survivors.
WITH batch AS (
    SELECT iata, path_key, node_ids, hash_prefix
    FROM known_routes
    ORDER BY last_reconfirmed_at
    LIMIT $1
),
amb AS MATERIALIZED (
    SELECT iata, 1 AS len, prefix_1 AS p FROM node_short_ids GROUP BY iata, prefix_1 HAVING COUNT(*) > 1
    UNION ALL
    SELECT iata, 2, prefix_2 FROM node_short_ids GROUP BY iata, prefix_2 HAVING COUNT(*) > 1
    UNION ALL
    SELECT iata, 3, prefix_3 FROM node_short_ids GROUP BY iata, prefix_3 HAVING COUNT(*) > 1
    UNION ALL
    SELECT iata, 4, prefix_4 FROM node_short_ids GROUP BY iata, prefix_4 HAVING COUNT(*) > 1
),
dead AS (
    SELECT b.iata, b.path_key
    FROM batch b
    WHERE EXISTS (
        SELECT 1
        FROM unnest(b.node_ids) AS hop_node_id
        WHERE NOT EXISTS (
            SELECT 1 FROM node_short_ids ns
            WHERE ns.node_id = hop_node_id
              AND ns.iata = b.iata
        )
    )
    UNION
    SELECT DISTINCT b.iata, b.path_key
    FROM batch b
    CROSS JOIN LATERAL unnest(b.hash_prefix) AS hp
    JOIN amb a ON a.iata = b.iata AND a.len = length(hp) AND a.p = hp
),
deleted AS (
    DELETE FROM known_routes kr
    USING dead d
    WHERE kr.iata = d.iata AND kr.path_key = d.path_key
)
UPDATE known_routes kr
SET last_reconfirmed_at = NOW()
FROM batch b
WHERE kr.iata = b.iata AND kr.path_key = b.path_key
  AND NOT EXISTS (
      SELECT 1 FROM dead d
      WHERE d.iata = b.iata AND d.path_key = b.path_key
  );

-- name: ReconfirmNeighbors :exec
-- Delete node_neighbors where the neighbor has departed from node_short_ids
-- for that IATA, or where its prefix_4 is now ambiguous.
DELETE FROM node_neighbors nn
WHERE NOT EXISTS (
    SELECT 1 FROM node_short_ids ns
    WHERE ns.node_id = nn.neighbor_id
      AND ns.iata = nn.iata
)
OR (
    SELECT COUNT(*) FROM node_short_ids ns
    WHERE ns.iata = nn.iata
      AND ns.prefix_4 = (
          SELECT prefix_4 FROM node_short_ids
          WHERE node_id = nn.neighbor_id
            AND iata = nn.iata
      )
) > 1;
