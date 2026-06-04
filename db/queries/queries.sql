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
UPDATE iata_codes SET
    display_name = $2,
    approx_lat   = $3,
    approx_lng   = $4
WHERE iata = $1;

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
    WHEN o.last_status_at > NOW() - INTERVAL '5 minutes' THEN 'online'
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
  ($1::text = '' OR (
      SELECT po.iata FROM packet_observations po
      WHERE po.observer_id = o.id
      ORDER BY po.heard_at DESC LIMIT 1
  ) = ANY(string_to_array($1::text, ',')))
  AND ($2 = '' OR o.observer_type = $2)
  AND ($3 = '' OR ob.broker_name = $3)
  AND ($4 = '' OR CASE
    WHEN o.last_status_at > NOW() - INTERVAL '5 minutes' THEN 'online'
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

-- name: GetObserverTelemetry :many
SELECT id, reported_at, battery_voltage_mv, airtime_tx_pct, airtime_rx_pct,
       noise_floor_db, uptime_seconds, queue_length, debug_flags, receive_errors
FROM observer_telemetry
WHERE observer_id = $1
  AND ($2::timestamptz IS NULL OR reported_at >= $2)
  AND ($3::timestamptz IS NULL OR reported_at <= $3)
  AND ($4 = 0 OR id > $4)
ORDER BY reported_at ASC;

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
  first_heard_at,
  last_heard_at
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), NOW()
)
ON CONFLICT (packet_hash) DO UPDATE SET
  last_heard_at = NOW()
RETURNING packet_hash, payload_type, payload_version, route_type, transport_codes_present, region_code, sub_region_code, origin_pubkey, raw_payload, raw_header, parsed_payload, decrypted, channel_hash, first_heard_at, last_heard_at, (xmax = 0)
AS inserted;

-- name: GetPacketByHash :one
SELECT p.*, ts.name AS scope_name
FROM packets p
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE p.packet_hash = $1;

-- name: GetPacketObservationCount :one
SELECT COUNT(*) FROM packet_observations WHERE packet_hash = $1;

-- name: ListPackets :many
-- Returns packets with the latest observation rolled in for display.
-- Pass cursor=0 to start from the beginning.
SELECT
  p.packet_hash,
  p.payload_type,
  p.route_type,
  p.first_heard_at,
  p.last_heard_at,
  p.scope_id,
  ts.name AS scope_name,
  (SELECT COUNT(*) FROM packet_observations po2 WHERE po2.packet_hash = p.packet_hash) AS observation_count,
  po.observer_id AS latest_observer_id,
  o.display_name AS latest_observer_name,
  po.iata AS latest_observer_iata
FROM packets p
LEFT JOIN LATERAL (
  SELECT observer_id, iata
  FROM packet_observations
  WHERE packet_hash = p.packet_hash
  ORDER BY heard_at DESC
  LIMIT 1
) po ON true
LEFT JOIN observers o ON o.id = po.observer_id
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE
  ($1::smallint = -1 OR p.payload_type = $1::smallint)
  AND ($2::smallint = -1 OR p.route_type = $2::smallint)
  AND ($3::text = '' OR EXISTS (
      SELECT 1 FROM packet_observations po3
      WHERE po3.packet_hash = p.packet_hash
      AND po3.iata = ANY(string_to_array($3::text, ','))
  ))
  AND ($4::timestamptz IS NULL OR p.first_heard_at >= $4)
  AND ($5::timestamptz IS NULL OR p.first_heard_at <= $5)
  AND ($6::timestamptz IS NULL OR p.last_heard_at < $6)
  AND ($8::text = '' OR ts.name = $8::text)
ORDER BY p.last_heard_at DESC
LIMIT $7;

-- name: ListPacketsAfterID :many
SELECT p.*
FROM packets p
JOIN packet_observations po ON po.packet_hash = p.packet_hash
WHERE po.id > $1
ORDER BY po.id ASC
LIMIT $2;


-- name: DeleteOldPackets :exec
-- Deletes packets and their observations older than the given cutoff.
-- packet_observations cascade-delete via FK.
DELETE FROM packets WHERE last_heard_at < $1;

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
  source_broker
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
)
ON CONFLICT (packet_hash, observer_id, heard_at) DO NOTHING
RETURNING *;

-- name: ListObservationsForPacket :many
SELECT po.*, o.display_name AS observer_name
FROM packet_observations po
LEFT JOIN observers o ON o.id = po.observer_id
WHERE po.packet_hash = $1
ORDER BY po.heard_at ASC;

-- name: ListObservationsForObserver :many
SELECT * FROM packet_observations
WHERE observer_id = $1
  AND ($2::timestamptz IS NULL OR heard_at >= $2)
ORDER BY heard_at DESC
LIMIT $3;

-- ============================================================
-- NODES
-- ============================================================

-- name: UpsertNode :one
INSERT INTO nodes (public_key, node_type, name, latitude, longitude, location_source, last_advert_at, last_seen, radio_freq_mhz, radio_sf, radio_bw_khz)
VALUES ($1, $2, $3, $4, $5, 'advert', NOW(), NOW(), $6, $7, $8)
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
  radio_bw_khz    = EXCLUDED.radio_bw_khz
RETURNING *;

-- name: SetNodeMultibytePaths :exec
UPDATE nodes SET supports_multibyte_paths = TRUE
WHERE id = $1 AND supports_multibyte_paths = FALSE;

-- name: SetNodeMultibyteTraces :exec
UPDATE nodes SET supports_multibyte_traces = TRUE
WHERE id = $1 AND supports_multibyte_traces = FALSE;

-- name: SetNodeDefaultScope :exec
UPDATE nodes SET default_scope_id = $2 WHERE id = $1;

-- name: GetNodeByPubkey :one
SELECT n.*, ts.name AS default_scope_name,
  EXISTS (SELECT 1 FROM observers o WHERE o.public_key = n.public_key) AS is_observer,
  (SELECT o.id FROM observers o WHERE o.public_key = n.public_key LIMIT 1) AS observer_id,
  (SELECT json_agg(json_build_object('iata', ni.iata, 'lastHeard', (extract(epoch from ni.last_heard) * 1000)::bigint) ORDER BY ni.last_heard DESC)
   FROM node_iatas ni WHERE ni.node_id = n.id) AS iatas
FROM nodes n
LEFT JOIN transport_scopes ts ON ts.id = n.default_scope_id
WHERE n.public_key = $1;

-- name: GetNodeByID :one
SELECT n.*, ts.name AS default_scope_name,
  EXISTS (SELECT 1 FROM observers o WHERE o.public_key = n.public_key) AS is_observer,
  (SELECT o.id FROM observers o WHERE o.public_key = n.public_key LIMIT 1) AS observer_id,
  (SELECT json_agg(json_build_object('iata', ni.iata, 'lastHeard', (extract(epoch from ni.last_heard) * 1000)::bigint) ORDER BY ni.last_heard DESC)
   FROM node_iatas ni WHERE ni.node_id = n.id) AS iatas
FROM nodes n
LEFT JOIN transport_scopes ts ON ts.id = n.default_scope_id
WHERE n.id = $1;


-- name: ListNodes :many
SELECT n.id, n.public_key, n.node_type, n.name, n.latitude, n.longitude, n.last_seen,
  n.radio_freq_mhz, n.radio_sf, n.radio_bw_khz,
  ts.name AS default_scope_name,
  json_agg(json_build_object('iata', ni.iata, 'lastHeard', (extract(epoch from ni.last_heard) * 1000)::bigint) ORDER BY ni.last_heard DESC) FILTER (WHERE ni.iata IS NOT NULL) AS iatas,
  EXISTS (SELECT 1 FROM observers o WHERE o.public_key = n.public_key) AS is_observer,
  (SELECT o.id FROM observers o WHERE o.public_key = n.public_key LIMIT 1) AS observer_id
FROM nodes n
LEFT JOIN node_iatas ni ON ni.node_id = n.id
LEFT JOIN transport_scopes ts ON ts.id = n.default_scope_id
WHERE
  ($1 = 0 OR n.node_type = $1)
  AND ($2::text = '' OR n.id IN (SELECT node_id FROM node_iatas WHERE iata = ANY(string_to_array($2::text, ','))))
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
GROUP BY n.id, ts.name
ORDER BY n.last_seen DESC
LIMIT $8;

-- name: GetNodeIATAs :many
SELECT iata FROM node_iatas WHERE node_id = $1 ORDER BY iata ASC;

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

-- name: SetChannelKeyKnown :exec
UPDATE channels SET key_known = TRUE
WHERE channel_hash = $1 AND key_fingerprint = $2;

-- name: ListChannels :many
-- Returns channels ordered by last seen, optionally filtered by hash and/or IATA.
-- Pass NULL for hash to skip hash filtering. Pass empty string for iata to skip IATA filtering.
-- IATA filter returns channels that have active packets in that IATA (case-insensitive).
-- Pass cursor=0 to start from the beginning (cursor is last_seen epoch ms).
SELECT DISTINCT c.* FROM channels c
WHERE ($1::bytea IS NULL OR c.channel_hash = $1)
  AND ($2 = '' OR EXISTS (
    SELECT 1 FROM packets p
    JOIN packet_observations po ON po.packet_hash = p.packet_hash
    WHERE p.channel_hash = c.channel_hash
      AND po.iata ILIKE $2
  ))
  AND ($3::timestamptz IS NULL OR c.last_seen < $3)
ORDER BY c.last_seen DESC
LIMIT $4;

-- name: GetChannelsByHash :many
-- Returns all channels for a given hash (may be multiple on hash collision).
SELECT * FROM channels WHERE channel_hash = $1 ORDER BY last_seen DESC LIMIT $2;

-- name: GetChannelByHashAndFingerprint :one
SELECT * FROM channels WHERE channel_hash = $1 AND key_fingerprint = $2;

-- name: GetChannelByHashtag :one
SELECT * FROM channels WHERE hashtag = $1;

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
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  AND ($4::text = '' OR ts.name = $4::text)
  AND ($5::bigint = 0 OR cm.id > $5::bigint)
ORDER BY cm.id ASC
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
  AND ($2::text = '' OR po.iata = ANY(string_to_array($2::text, ',')))
  AND ($3::text = '' OR ts.name = $3::text)
  AND ($4 = 0 OR cm.id > $4)
ORDER BY cm.id ASC
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
  AND ($3::text = '' OR po.iata = ANY(string_to_array($3::text, ',')))
  AND ($4::text = '' OR ts.name = $4::text)
  AND ($5::bigint = 0 OR cm.id > $5::bigint)
ORDER BY cm.id ASC
LIMIT $6;

-- name: InsertObserverTelemetry :exec
-- Inserts a telemetry snapshot for an observer. The reported_at timestamp should
-- be truncated to the configured resolution before calling to ensure deduplication.
INSERT INTO observer_telemetry (
    observer_id, reported_at, battery_voltage_mv, airtime_tx_pct,
    airtime_rx_pct, noise_floor_db, uptime_seconds, queue_length,
    debug_flags, receive_errors
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (observer_id, reported_at) DO NOTHING;

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
  AND ($1 = '' OR po.iata ILIKE $1);

-- name: GetHourlyStats :many
SELECT iata, hour, observation_count, unique_packets, active_observers
FROM mv_hourly_iata_stats
WHERE ($1 = '' OR iata ILIKE $1)
  AND hour >= NOW() - $2::interval
ORDER BY iata, hour;

-- name: GetTopNodes :many
SELECT * FROM mv_top_nodes_by_iata
WHERE ($1::char(3) IS NULL OR iata = $1)
ORDER BY observation_count DESC
LIMIT $2;

-- name: GetStatsPayloadBreakdown :many
-- Returns observation counts grouped by payload type for the given window and IATA.
SELECT
  p.payload_type,
  COUNT(*) AS count
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
WHERE po.heard_at > $1
  AND ($2 = '' OR po.iata ILIKE $2)
GROUP BY p.payload_type
ORDER BY count DESC;

-- name: GetStatsTopObservers :many
-- Returns the top N observers by observation count for the given window and IATA.
SELECT
  o.id,
  o.display_name,
  o.observer_type,
  COUNT(*) AS observation_count,
  COALESCE((
    SELECT po2.iata FROM packet_observations po2
    WHERE po2.observer_id = o.id
    ORDER BY po2.heard_at DESC LIMIT 1
  ), '') AS iata
FROM packet_observations po
JOIN observers o ON o.id = po.observer_id
WHERE po.heard_at > $1
  AND ($2 = '' OR po.iata ILIKE $2)
GROUP BY o.id
ORDER BY observation_count DESC
LIMIT $3;

-- name: GetRadioPresets :many
SELECT preset, iata, source_type, count
FROM mv_radio_presets
WHERE ($1::text = '' OR preset = $1::text)
  AND ($2::text = '' OR iata = $2::text)
ORDER BY preset, iata, source_type;

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
-- HELPERS
-- ============================================================

-- name: ResolvePathHashes :many
SELECT ns.prefix_4 AS hash, n.id AS node_id, n.name, n.latitude, n.longitude, n.public_key
FROM node_short_ids ns
JOIN nodes n ON n.id = ns.node_id
WHERE ns.iata = $1
  AND CASE
    WHEN cardinality($2::bytea[]) > 0 AND length($2[1]) = 1 THEN ns.prefix_1 = ANY($2)
    WHEN cardinality($2::bytea[]) > 0 AND length($2[1]) = 2 THEN ns.prefix_2 = ANY($2)
    WHEN cardinality($2::bytea[]) > 0 AND length($2[1]) = 3 THEN ns.prefix_3 = ANY($2)
    WHEN cardinality($2::bytea[]) > 0 AND length($2[1]) = 4 THEN ns.prefix_4 = ANY($2)
    ELSE FALSE
  END;

-- name: RefreshHourlyStats :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_hourly_iata_stats;

-- name: RefreshTopNodes :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_top_nodes_by_iata;

-- name: RefreshRadioPresets :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_radio_presets;
