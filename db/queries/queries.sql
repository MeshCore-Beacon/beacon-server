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
-- OBSERVERS
-- ============================================================

-- name: UpsertObserver :one
INSERT INTO observers (public_key, last_seen)
VALUES ($1, NOW())
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

-- name: GetObserverByPubkey :one
SELECT * FROM observers WHERE public_key = $1;

-- name: ListObservers :many
SELECT * FROM observers ORDER BY last_seen DESC;

-- name: GetObserverLastIATA :one
SELECT iata FROM packet_observations
WHERE observer_id = $1
ORDER BY heard_at DESC
LIMIT 1;

-- name: GetObserverRadio :one
SELECT radio_freq_mhz, radio_bw_khz, radio_sf, radio_cr
FROM observers
WHERE id = $1;

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
  parsed_payload,
  channel_hash,
  first_heard_at,
  last_heard_at,
  observation_count
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NOW(), NOW(), 1
)
ON CONFLICT (packet_hash) DO UPDATE SET
  last_heard_at     = NOW(),
  observation_count = packets.observation_count + 1
RETURNING *, (xmax = 0) AS inserted;

-- name: GetPacket :one
SELECT * FROM packets WHERE packet_hash = $1;

-- name: ListPackets :many
SELECT p.*
FROM packets p
WHERE
  ($1::smallint IS NULL OR p.payload_type = $1)
  AND ($2::smallint IS NULL OR p.route_type = $2)
  AND ($3::timestamptz IS NULL OR p.first_heard_at >= $3)
  AND ($4::timestamptz IS NULL OR p.first_heard_at <= $4)
ORDER BY p.last_heard_at DESC
LIMIT $5;

-- name: ListPacketsAfterID :many
SELECT p.*
FROM packets p
JOIN packet_observations po ON po.packet_hash = p.packet_hash
WHERE po.id > $1
ORDER BY po.id ASC
LIMIT $2;

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
SELECT * FROM packet_observations
WHERE packet_hash = $1
ORDER BY heard_at ASC;

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
INSERT INTO nodes (public_key, node_type, name, latitude, longitude, location_source, last_advert_at, last_seen)
VALUES ($1, $2, $3, $4, $5, 'advert', NOW(), NOW())
ON CONFLICT (public_key) DO UPDATE SET
  node_type       = EXCLUDED.node_type,
  name            = COALESCE(EXCLUDED.name, nodes.name),
  latitude        = COALESCE(EXCLUDED.latitude, nodes.latitude),
  longitude       = COALESCE(EXCLUDED.longitude, nodes.longitude),
  location_source = CASE WHEN EXCLUDED.latitude IS NOT NULL THEN 'advert' ELSE nodes.location_source END,
  last_advert_at  = NOW(),
  last_seen       = NOW()
RETURNING *;

-- name: SetNodeMultibytePaths :exec
UPDATE nodes SET supports_multibyte_paths = TRUE
WHERE id = $1 AND supports_multibyte_paths = FALSE;

-- name: SetNodeMultibyteTraces :exec
UPDATE nodes SET supports_multibyte_traces = TRUE
WHERE id = $1 AND supports_multibyte_traces = FALSE;

-- name: GetNodeByPubkey :one
SELECT * FROM nodes WHERE public_key = $1;

-- name: ListNodes :many
SELECT * FROM nodes
WHERE
  ($1::smallint IS NULL OR node_type = $1)
ORDER BY last_seen DESC
LIMIT $2;

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

-- name: SetChannelKeyKnown :exec
UPDATE channels SET key_known = TRUE
WHERE channel_hash = $1 AND key_fingerprint = $2;

-- name: ListChannels :many
SELECT * FROM channels ORDER BY last_seen DESC LIMIT $1;

-- name: GetChannelsByHash :many
-- Returns all channels for a given hash (may be multiple on hash collision).
SELECT * FROM channels WHERE channel_hash = $1 ORDER BY last_seen DESC;

-- name: GetChannelByHashAndFingerprint :one
SELECT * FROM channels WHERE channel_hash = $1 AND key_fingerprint = $2;

-- name: GetChannelByHashtag :one
SELECT * FROM channels WHERE hashtag = $1;

-- ============================================================
-- CHANNEL MESSAGES
-- ============================================================

-- name: InsertChannelMessage :exec
INSERT INTO channel_messages (channel_id, packet_hash, sender_name, sender_pubkey, content, sent_at)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (packet_hash) DO NOTHING;

-- name: ListChannelMessages :many
SELECT * FROM channel_messages
WHERE channel_id = $1
  AND ($2::timestamptz IS NULL OR sent_at >= $2)
ORDER BY sent_at DESC
LIMIT $3;

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
  AND ($1::char(3) IS NULL OR po.iata = $1);

-- name: GetHourlyStats :many
SELECT * FROM mv_hourly_iata_stats
WHERE ($1::char(3) IS NULL OR iata = $1)
  AND hour >= NOW() - $2::interval
ORDER BY iata, hour;

-- name: GetTopNodes :many
SELECT * FROM mv_top_nodes_by_iata
WHERE ($1::char(3) IS NULL OR iata = $1)
ORDER BY observation_count DESC
LIMIT $2;

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
SELECT DISTINCT n.id
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
