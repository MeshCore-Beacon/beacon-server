-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: agpl

-- ============================================================
-- IATA CODES
-- ============================================================

-- name: UpsertIATA :exec
INSERT INTO iata_codes (iata)
VALUES ($1)
ON CONFLICT (iata) DO NOTHING;

-- name: AddIATAs :exec
INSERT INTO iata_codes (iata)
SELECT unnest(@iatas::bpchar[])
ON CONFLICT (iata) DO NOTHING;

-- name: GetIATA :one
SELECT * FROM iata_codes WHERE iata = $1;

-- name: ListIATAs :many
SELECT * FROM iata_codes ORDER BY iata;

-- name: ListHeardIATAs :many
SELECT DISTINCT last_iata::text AS iata FROM observers WHERE last_iata IS NOT NULL ORDER BY 1;

-- name: UpsertIATADetails :exec
INSERT INTO iata_codes (iata, display_name, approx_lat, approx_lng)
VALUES ($1, $2, $3, $4)
ON CONFLICT (iata) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    approx_lat   = EXCLUDED.approx_lat,
    approx_lng   = EXCLUDED.approx_lng;

-- name: GetIATABorder :one
-- An imported MeshMapper boundary overrides the configured one. border is NULL
-- when neither exists; a missing row (unknown IATA) is sql.ErrNoRows, same
-- not-found distinction GetIATA already makes.
SELECT COALESCE(z.feature, i.border)::jsonb AS border
FROM iata_codes i
LEFT JOIN meshmapper_zone_boundaries z ON z.iata = i.iata
WHERE i.iata = $1;

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
  key_fingerprint = EXCLUDED.key_fingerprint,
  imported_only   = FALSE;

-- name: GetTransportScopes :many
SELECT name, transport_key, key_fingerprint FROM transport_scopes WHERE NOT imported_only ORDER BY name;

-- name: GetTransportScopeByName :one
SELECT id FROM transport_scopes WHERE name = $1;

-- name: GetScopeNames :many
SELECT name FROM transport_scopes ORDER BY name;

-- name: GetScopeByName :one
-- Packets and IATAs cover every retained rollup hour; IATAs are those that heard the
-- scope's own packets. Observer and node counts are current memberships.
SELECT
    ts.name,
    COALESCE((SELECT SUM(s.packets) FROM analytics_hourly_scope_sets s WHERE s.scope_id = ts.id), 0)::bigint AS packet_count,
    (SELECT COUNT(*) FROM observer_scopes os WHERE os.scope_id = ts.id)::bigint AS observer_count,
    (SELECT COUNT(*) FROM nodes n WHERE n.default_scope_id = ts.id)::bigint AS node_count,
    cardinality(i.iatas)::bigint AS iata_count,
    i.iatas
FROM transport_scopes ts
CROSS JOIN LATERAL (
    SELECT COALESCE(array_agg(DISTINCT x.iata::text ORDER BY x.iata::text), '{}')::text[] AS iatas
    FROM analytics_hourly_scope_sets s CROSS JOIN LATERAL unnest(s.iatas) AS x(iata)
    WHERE s.scope_id = ts.id
) i
WHERE ts.name = $1;

-- ============================================================
-- OBSERVERS
-- ============================================================

-- name: UpsertObserver :one
-- Empty iata (status/neighbors) leaves last_iata alone; otherwise the newest iata_at wins.
INSERT INTO observers (public_key, observer_type, last_seen, last_iata, last_iata_at)
VALUES ($1, 'unknown', NOW(), NULLIF(sqlc.arg(iata)::text, ''),
        CASE WHEN sqlc.arg(iata)::text <> '' THEN sqlc.arg(iata_at)::timestamptz END)
ON CONFLICT (public_key) DO UPDATE SET
  last_seen         = NOW(),
  observation_count = observers.observation_count + 1,
  last_iata         = CASE WHEN EXCLUDED.last_iata IS NOT NULL
                            AND (observers.last_iata_at IS NULL OR EXCLUDED.last_iata_at > observers.last_iata_at)
                           THEN EXCLUDED.last_iata ELSE observers.last_iata END,
  last_iata_at      = GREATEST(observers.last_iata_at, EXCLUDED.last_iata_at)
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
  observation_count = COALESCE(o.observation_count, 0) + v.delta,
  last_iata         = CASE WHEN v.iata <> '' AND (o.last_iata_at IS NULL OR v.iata_at > o.last_iata_at)
                           THEN v.iata ELSE o.last_iata END,
  last_iata_at      = GREATEST(o.last_iata_at, v.iata_at)
FROM (
  SELECT unnest($1::uuid[]) AS id,
         unnest($2::timestamptz[]) AS seen,
         unnest($3::int[]) AS delta,
         unnest($4::text[]) AS iata,
         unnest($5::timestamptz[]) AS iata_at
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
COALESCE(o.last_iata, '')::text AS iata
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
SELECT COALESCE(last_iata, '')::text FROM observers WHERE id = $1;

-- name: GetObserverRadio :one
SELECT radio_freq_mhz, radio_bw_khz, radio_sf, radio_cr
FROM observers
WHERE id = $1;

-- name: InsertObserverTelemetry :exec
-- Inserts a telemetry snapshot for an observer. The reported_at timestamp should
-- be truncated to the configured resolution before calling to ensure deduplication.
INSERT INTO observer_telemetry (
    observer_id, reported_at, battery_voltage_mv, airtime_tx_secs,
    airtime_rx_secs, noise_floor_db, uptime_seconds, queue_length,
    debug_flags, receive_errors
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (observer_id, reported_at) DO NOTHING;

-- name: GetObserverTelemetry :many
SELECT id, reported_at, battery_voltage_mv, airtime_tx_secs, airtime_rx_secs,
       noise_floor_db, uptime_seconds, queue_length, debug_flags, receive_errors
FROM observer_telemetry
WHERE observer_id = $1
  AND ($2::timestamptz IS NULL OR reported_at >= $2)
  AND ($3::timestamptz IS NULL OR reported_at <= $3)
  AND ($4 = 0 OR id > $4)
ORDER BY reported_at ASC;

-- name: GetObserverActivityRaw :many
-- Sub-hour activity buckets straight off idx_observations_observer; no join to packets.
-- Aggregates are COALESCEd and paired with a count column: sqlc types a cast expression as
-- NOT NULL, so the counts are what tell the store a bucket had no costed or no signal rows.
SELECT
  date_bin($3::interval, heard_at, TIMESTAMPTZ 'epoch')::timestamptz AS bucket,
  COUNT(*)::bigint AS observations,
  COALESCE(SUM(airtime_ms), 0)::real AS airtime_ms,
  COUNT(airtime_ms)::bigint AS airtime_n,
  COALESCE(AVG(snr)  FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0)), 0)::real AS snr_avg,
  COALESCE(MIN(snr)  FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0)), 0)::real AS snr_min,
  COUNT(snr)         FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::bigint AS snr_n,
  COALESCE(AVG(rssi) FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0)), 0)::real AS rssi_avg,
  COUNT(rssi)        FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::bigint AS rssi_n
FROM packet_observations
WHERE observer_id = $1 AND heard_at >= $2::timestamptz AND heard_at < @until::timestamptz
GROUP BY bucket
ORDER BY bucket;

-- name: GetObserverActivityRawPayloadTypes :many
SELECT payload_type, COUNT(*)::bigint AS count
FROM packet_observations
WHERE observer_id = $1 AND heard_at >= $2::timestamptz AND heard_at < @until::timestamptz
GROUP BY payload_type
ORDER BY count DESC;

-- name: GetObserverActivityHourly :many
-- Hour-or-coarser buckets summed from the hourly rollup; same COALESCE-plus-count shape as the raw query.
-- Hours from @tail_start on aren't rolled yet and read raw rows.
WITH src AS (
  SELECT a.hour AS t, a.observations, a.airtime_ms, a.airtime_n, a.snr_sum, a.snr_n, a.snr_min, a.rssi_sum, a.rssi_n
  FROM analytics_hourly_observer_activity a
  WHERE a.observer_id = $1 AND a.hour >= $2::timestamptz AND a.hour < LEAST(@until::timestamptz, @tail_start::timestamptz)
  UNION ALL
  SELECT o.heard_at, 1::bigint, o.airtime_ms, (o.airtime_ms IS NOT NULL)::int::bigint,
         s.snr, (s.snr IS NOT NULL)::int::bigint, s.snr, s.rssi::bigint, (s.rssi IS NOT NULL)::int::bigint
  FROM packet_observations o CROSS JOIN
       LATERAL (SELECT CASE WHEN NOT (COALESCE(o.rssi, 0) = 0 AND COALESCE(o.snr, 0) = 0) THEN o.snr END AS snr,
                       CASE WHEN NOT (COALESCE(o.rssi, 0) = 0 AND COALESCE(o.snr, 0) = 0) THEN o.rssi END AS rssi) s
  WHERE o.observer_id = $1 AND o.heard_at >= GREATEST($2::timestamptz, @tail_start::timestamptz) AND o.heard_at < @until::timestamptz
)
SELECT
  date_bin($3::interval, t, TIMESTAMPTZ 'epoch')::timestamptz AS bucket,
  SUM(observations)::bigint AS observations,
  COALESCE(SUM(airtime_ms), 0)::real AS airtime_ms,
  SUM(airtime_n)::bigint AS airtime_n,
  COALESCE(SUM(snr_sum), 0)::real AS snr_sum,
  SUM(snr_n)::bigint AS snr_n,
  COALESCE(MIN(snr_min), 0)::real AS snr_min,
  COALESCE(SUM(rssi_sum), 0)::bigint AS rssi_sum,
  SUM(rssi_n)::bigint AS rssi_n
FROM src
GROUP BY 1
ORDER BY 1;

-- name: GetObserverActivityHourlyPayloadTypes :many
-- Same rollup/raw-tail split as GetObserverActivityHourly.
WITH src AS (
  SELECT a.payload_type, a.observations AS n
  FROM analytics_hourly_observer_activity a
  WHERE a.observer_id = $1 AND a.hour >= $2::timestamptz AND a.hour < LEAST(@until::timestamptz, @tail_start::timestamptz)
  UNION ALL
  SELECT COALESCE(o.payload_type, -1)::smallint, 1::bigint
  FROM packet_observations o
  WHERE o.observer_id = $1 AND o.heard_at >= GREATEST($2::timestamptz, @tail_start::timestamptz) AND o.heard_at < @until::timestamptz
)
SELECT payload_type, SUM(n)::bigint AS count
FROM src
GROUP BY payload_type
ORDER BY count DESC;

-- name: ListObserverAdverts :many
-- Returns advert packets (payload_type=4) heard by a specific observer.
-- Pass cursor=0 to start from the beginning, or the last seen id for pagination.
-- Keep missing-origin adverts; the generated key field expects a string, not NULL.
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
  COALESCE(encode(p.origin_pubkey, 'hex'), '')::text AS node_public_key
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

-- name: DeleteOldObservers :many
-- Opt-in age-out: preserve retained history and manually recorded ownership.
-- Bound deletions per cleanup tick and skip observers being updated by ingest.
WITH expired AS (
    SELECT o.id
    FROM observers o
    WHERE o.last_seen < $1
      AND (o.last_status_at IS NULL OR o.last_status_at < $1)
      AND NOT EXISTS (SELECT 1 FROM packet_observations po WHERE po.observer_id = o.id)
      AND NOT EXISTS (SELECT 1 FROM observer_telemetry ot WHERE ot.observer_id = o.id)
      AND NOT EXISTS (SELECT 1 FROM observer_owners oo WHERE oo.observer_id = o.id)
    ORDER BY o.last_seen, o.id
    LIMIT 1000
    FOR UPDATE OF o SKIP LOCKED
)
DELETE FROM observers o USING expired e
WHERE o.id = e.id
RETURNING o.id;

-- ============================================================
-- OBSERVER BROKERS
-- ============================================================

-- name: UpsertObserverBroker :exec
INSERT INTO observer_brokers (observer_id, broker_name, last_seen, last_packet_at)
VALUES ($1, $2, NOW(), CASE WHEN @is_packet::boolean THEN NOW() END)
ON CONFLICT (observer_id, broker_name) DO UPDATE SET
  last_seen = NOW(),
  last_packet_at = COALESCE(EXCLUDED.last_packet_at, observer_brokers.last_packet_at);

-- name: TouchObserverBrokers :exec
UPDATE observer_brokers ob SET
  last_seen      = GREATEST(ob.last_seen, v.seen),
  last_packet_at = GREATEST(ob.last_packet_at, v.packet)
FROM (
  SELECT unnest($1::uuid[]) AS observer_id,
         unnest($2::text[]) AS broker_name,
         unnest($3::timestamptz[]) AS seen,
         unnest($4::timestamptz[]) AS packet
) v
WHERE ob.observer_id = v.observer_id AND ob.broker_name = v.broker_name;

-- ============================================================
-- PACKETS
-- ============================================================

-- name: UpsertPacket :one
-- A new TRACE packet adds itself to its tag summary in the same statement, so the summary
-- can't miss a stored packet. 'TRACE' > 'PING'; the longest path keeps the best payload.
WITH up AS (
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
RETURNING packet_hash, payload_type, payload_version, route_type, transport_codes_present, region_code, sub_region_code, origin_pubkey, raw_payload, raw_header, parsed_payload, decrypted, channel_hash, first_heard_at, last_heard_at, trace_tag, scope_id, (xmax = 0)
AS inserted
), trace AS (
  INSERT INTO trace_tags (trace_tag, first_heard_at, last_heard_at, packet_count, trace_type, scope_id, best_payload, best_path_len)
  SELECT u.trace_tag, u.first_heard_at, u.last_heard_at, 1, u.parsed_payload->>'type', u.scope_id, u.parsed_payload,
         CASE WHEN jsonb_typeof(u.parsed_payload->'pathHashes') = 'array'
              THEN jsonb_array_length(u.parsed_payload->'pathHashes') ELSE 0 END
  FROM up u
  WHERE u.inserted AND u.trace_tag IS NOT NULL
  ON CONFLICT (trace_tag) DO UPDATE SET
    packet_count   = trace_tags.packet_count + 1,
    trace_type     = GREATEST(trace_tags.trace_type, EXCLUDED.trace_type),
    scope_id       = COALESCE(trace_tags.scope_id, EXCLUDED.scope_id),
    best_payload   = CASE WHEN EXCLUDED.best_path_len > trace_tags.best_path_len
                          THEN EXCLUDED.best_payload ELSE trace_tags.best_payload END,
    best_path_len  = GREATEST(trace_tags.best_path_len, EXCLUDED.best_path_len)
)
SELECT packet_hash, payload_type, payload_version, route_type, transport_codes_present, region_code, sub_region_code, origin_pubkey, raw_payload, raw_header, parsed_payload, decrypted, channel_hash, first_heard_at, last_heard_at, inserted
FROM up;

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
-- Return distinct observation IATAs in first-heard order for path resolution,
-- without fetching full observations separately for every trace packet.
SELECT encode(p.packet_hash, 'hex') AS packet_hash_hex,
    p.route_type,
    p.first_heard_at,
    p.last_heard_at,
    p.parsed_payload,
    p.scope_id,
    ts.name AS scope_name,
    ARRAY(
        SELECT po.iata
        FROM packet_observations po
        WHERE po.packet_hash = p.packet_hash
        GROUP BY po.iata
        ORDER BY MIN(po.heard_at), po.iata
    )::bpchar[] AS iatas
FROM packets p
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE p.trace_tag = decode($1, 'hex')
ORDER BY p.first_heard_at ASC;

-- name: ListPackets :many
-- Returns packets with the latest observation rolled in for display.
-- Pass cursor=0 to start from the beginning. IATA-filtered requests are
-- served by ListPacketsByIATAs instead.
SELECT
  p.packet_hash,
  p.payload_type,
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
  p.route_type,
  p.first_heard_at,
  p.last_heard_at,
  p.scope_id,
  ts.name AS scope_name,
  p.observation_count AS observation_count,
  -- sqlc loses LATERAL nullability; these scalar defaults are ignored when observer_id is NULL.
  po.observer_id AS latest_observer_id,
  o.display_name AS latest_observer_name,
  COALESCE(po.iata, ''::bpchar) AS latest_observer_iata,
  COALESCE(po.path_length_byte, 0::smallint) AS latest_observer_path_length_byte,
  COALESCE(po.hash_size, 0::smallint) AS latest_observer_hash_size,
  COALESCE(po.hop_count, 0::smallint) AS latest_observer_hop_count,
  po.path_bytes AS latest_observer_path_bytes,
  p.raw_payload,
  p.origin_pubkey
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
  p.route_type,
  p.first_heard_at,
  p.last_heard_at,
  p.scope_id,
  ts.name AS scope_name,
  sh.site_heard_at,
  sat.scan_saturated,
  sat.scan_floor,
  p.observation_count AS observation_count,
  -- sqlc loses LATERAL nullability; these scalar defaults are ignored when observer_id is NULL.
  po.observer_id AS latest_observer_id,
  o.display_name AS latest_observer_name,
  COALESCE(po.iata, ''::bpchar) AS latest_observer_iata,
  COALESCE(po.path_length_byte, 0::smallint) AS latest_observer_path_length_byte,
  COALESCE(po.hash_size, 0::smallint) AS latest_observer_hash_size,
  COALESCE(po.hop_count, 0::smallint) AS latest_observer_hop_count,
  po.path_bytes AS latest_observer_path_bytes,
  p.raw_payload,
  p.origin_pubkey
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
  p.route_type,
  p.first_heard_at,
  p.last_heard_at,
  p.observation_count AS observation_count,
  po.observer_id AS latest_observer_id,
  o.display_name AS latest_observer_name,
  po.iata AS latest_observer_iata,
  po.path_length_byte AS latest_observer_path_length_byte,
  po.hash_size AS latest_observer_hash_size,
  po.hop_count AS latest_observer_hop_count,
  po.path_bytes AS latest_observer_path_bytes,
  p.raw_payload,
  p.origin_pubkey,
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

-- name: DeleteOldPackets :one
-- Deletes one bounded cohort (observations cascade). SKIP LOCKED leaves packets an in-flight
-- observation insert holds. raw_deleted_before tells the rollup which hours lost raw rows.
WITH victims AS MATERIALIZED (
  SELECT vp.packet_hash, vp.last_heard_at FROM packets vp
  WHERE vp.last_heard_at < @cutoff::timestamptz
  ORDER BY vp.last_heard_at, vp.packet_hash
  LIMIT @batch_size::integer
  FOR UPDATE OF vp SKIP LOCKED
), deleted AS (
  DELETE FROM packets dp USING victims v WHERE dp.packet_hash = v.packet_hash
  RETURNING dp.last_heard_at
), mark AS (
  UPDATE analytics_raw_state
  SET raw_deleted_before = GREATEST(raw_deleted_before, (SELECT max(last_heard_at) FROM deleted))
  WHERE EXISTS (SELECT 1 FROM deleted)
)
SELECT count(*) FROM deleted;

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

-- name: DeleteOldRoutes :execrows
-- One batch of routes past retention, or past grace with too few observations.
-- GREATEST keeps the scan on idx_known_routes_last_seen.
WITH expired AS (
    SELECT r.iata, r.path_key
    FROM known_routes r
    WHERE r.last_seen < GREATEST(@retention_cutoff::timestamptz, @grace_cutoff::timestamptz)
      AND (r.last_seen < @retention_cutoff OR
           (r.observation_count < @min_observations AND r.last_seen < @grace_cutoff))
    LIMIT @batch_size
    FOR UPDATE OF r SKIP LOCKED
)
DELETE FROM known_routes kr USING expired e
WHERE kr.iata = e.iata AND kr.path_key = e.path_key;

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
-- Bumps packets.observation_count only for a new row; a duplicate returns the current count.
WITH ins AS (
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
  payload_type,
  airtime_ms
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
)
ON CONFLICT (packet_hash, observer_id) DO NOTHING
RETURNING packet_hash
), bump AS (
  UPDATE packets p SET observation_count = p.observation_count + 1
  FROM ins WHERE p.packet_hash = ins.packet_hash
  RETURNING p.observation_count
)
SELECT EXISTS (SELECT 1 FROM ins) AS inserted,
       COALESCE((SELECT b.observation_count FROM bump b),
                (SELECT pk.observation_count FROM packets pk WHERE pk.packet_hash = $1), 0)::bigint AS observation_count;

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
VALUES (@public_key, @node_type, @name, @latitude, @longitude, CASE WHEN @latitude::double precision IS NOT NULL THEN 'advert' END, NOW(), NOW(), @radio_freq_mhz, @radio_sf, @radio_bw_khz, @device_clock_drift_seconds)
ON CONFLICT (public_key) DO UPDATE SET
  node_type       = EXCLUDED.node_type,
  name            = COALESCE(EXCLUDED.name, nodes.name),
  latitude        = CASE WHEN @clear_location::bool THEN NULL ELSE COALESCE(EXCLUDED.latitude, nodes.latitude) END,
  longitude       = CASE WHEN @clear_location::bool THEN NULL ELSE COALESCE(EXCLUDED.longitude, nodes.longitude) END,
  location_source = CASE WHEN @clear_location::bool THEN NULL WHEN EXCLUDED.latitude IS NOT NULL THEN 'advert' ELSE nodes.location_source END,
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

-- name: GetNodesByPubkeys :many
SELECT id, public_key, name, latitude, longitude
FROM nodes
WHERE public_key = ANY(@pubkeys::bytea[]);

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
  name          = COALESCE(EXCLUDED.name, channels.name)
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

-- name: ListUndecryptedGroupTextPacketsByHash :many
-- Like ListUndecryptedGroupTextPackets, limited to channels that just gained a key.
SELECT packet_hash, raw_payload FROM packets
WHERE payload_type = 5 AND decrypted IS NOT TRUE AND channel_hash = ANY(@hashes::bytea[]);

-- name: DeleteChannelConfigScopes :exec
DELETE FROM channel_config_scopes;

-- name: AddChannelConfigScopes :exec
-- An empty region places the channel Beacon-wide.
INSERT INTO channel_config_scopes (key_fingerprint, region_slug)
SELECT unnest(@fingerprints::bytea[]), NULLIF(unnest(@regions::text[]), '');

-- name: UpsertChannelIATA :exec
-- Refreshes at most hourly so repeat hears don't churn the row.
INSERT INTO channel_iatas (channel_hash, iata, last_heard)
VALUES ($1, $2, $3)
ON CONFLICT (channel_hash, iata) DO UPDATE SET
  last_heard = EXCLUDED.last_heard
WHERE EXCLUDED.last_heard > channel_iatas.last_heard + INTERVAL '1 hour';

-- name: RecordTrace :exec
-- One hearing of a TRACE packet: trace_iatas refreshes at most hourly and the tag's heard
-- window widens (the first hearing replaces the packet's provisional times). Packet counts
-- and payloads come from UpsertPacket.
WITH iata AS (
  INSERT INTO trace_iatas (trace_tag, iata, last_heard)
  VALUES (@trace_tag, @iata, @heard_at)
  ON CONFLICT (trace_tag, iata) DO UPDATE SET
    last_heard = EXCLUDED.last_heard
  WHERE EXCLUDED.last_heard > trace_iatas.last_heard + INTERVAL '1 hour'
)
INSERT INTO trace_tags (trace_tag, first_heard_at, last_heard_at, heard)
VALUES (@trace_tag, @heard_at, @heard_at, true)
ON CONFLICT (trace_tag) DO UPDATE SET
  first_heard_at = CASE WHEN trace_tags.heard THEN LEAST(trace_tags.first_heard_at, EXCLUDED.first_heard_at)
                        ELSE EXCLUDED.first_heard_at END,
  last_heard_at  = CASE WHEN trace_tags.heard THEN GREATEST(trace_tags.last_heard_at, EXCLUDED.last_heard_at)
                        ELSE EXCLUDED.last_heard_at END,
  heard          = true;

-- name: DeleteOldTraceTags :exec
DELETE FROM trace_tags WHERE last_heard_at < $1;

-- name: ListChannels :many
-- Channels ordered by last seen, optionally filtered by hash and/or IATAs.
-- A channel belongs to an IATA when MeshMapper lists it there or config scopes it
-- to a region containing it (or Beacon-wide). NULL hash / empty array / NULL key_known skip those filters.
-- Pass cursor=0 to start from the beginning (cursor is last_seen epoch ms).
SELECT c.* FROM channels c
WHERE (@channel_hash::bytea IS NULL OR c.channel_hash = @channel_hash)
  AND (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0
    OR EXISTS (SELECT 1 FROM meshmapper_channel_members m
      WHERE m.key_fingerprint = c.key_fingerprint AND m.iata::bpchar = ANY(@iatas::bpchar[]))
    OR EXISTS (SELECT 1 FROM channel_config_scopes s
      WHERE s.key_fingerprint = c.key_fingerprint AND (s.region_slug IS NULL OR s.region_slug IN (
        SELECT r.slug FROM regions r JOIN region_iatas ri ON ri.region_id = r.id
        WHERE ri.iata = ANY(@iatas::bpchar[])))))
  AND (sqlc.narg(key_known)::boolean IS NULL OR COALESCE(c.key_known, false) = sqlc.narg(key_known))
  AND (@cursor_ts::timestamptz IS NULL OR c.last_seen < @cursor_ts)
ORDER BY c.last_seen DESC, c.id DESC
LIMIT @page_limit;

-- name: ListChannelsAfter :many
-- Keep the non-null tuple boundary separate from the legacy optional cursor so
-- generic prepared plans can seek directly into the composite ordered index.
SELECT c.* FROM channels c
WHERE (c.last_seen, c.id) < (@cursor_ts::timestamptz, @cursor_id::integer)
  AND (@channel_hash::bytea IS NULL OR c.channel_hash = @channel_hash)
  AND (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0
    OR EXISTS (SELECT 1 FROM meshmapper_channel_members m
      WHERE m.key_fingerprint = c.key_fingerprint AND m.iata::bpchar = ANY(@iatas::bpchar[]))
    OR EXISTS (SELECT 1 FROM channel_config_scopes s
      WHERE s.key_fingerprint = c.key_fingerprint AND (s.region_slug IS NULL OR s.region_slug IN (
        SELECT r.slug FROM regions r JOIN region_iatas ri ON ri.region_id = r.id
        WHERE ri.iata = ANY(@iatas::bpchar[])))))
  AND (sqlc.narg(key_known)::boolean IS NULL OR COALESCE(c.key_known, false) = sqlc.narg(key_known))
ORDER BY c.last_seen DESC, c.id DESC
LIMIT @page_limit;

-- name: GetChannelByID :one
SELECT * FROM channels WHERE id = $1;

-- ============================================================
-- CHANNEL MESSAGES
-- ============================================================

-- name: InsertChannelMessage :one
-- Read the immutable first-packet scope in the same statement as insertion.
-- A later reception's transport code must not give live and historical messages different tags.
-- message_count is a lifetime count, bumped only for a new message. A historical (backfilled)
-- message queues its observation hours for a re-roll in the same statement, so the two can't diverge.
WITH inserted AS (
  INSERT INTO channel_messages (channel_id, packet_hash, sender_name, content, sent_at)
  VALUES ($1, $2, $3, $4, $5)
  ON CONFLICT (packet_hash) DO NOTHING
  RETURNING id, packet_hash, channel_id
), bump AS (
  UPDATE channels c SET message_count = c.message_count + 1
  FROM inserted WHERE c.id = inserted.channel_id
), dirty AS (
  INSERT INTO analytics_dirty_hours (hour)
  SELECT DISTINCT date_trunc('hour', po.heard_at, 'UTC')
  FROM packet_observations po JOIN inserted i ON i.packet_hash = po.packet_hash
  WHERE $6::boolean
  ON CONFLICT (hour) DO UPDATE SET enqueued_at = now()
)
SELECT inserted.id, ts.name AS scope_name, p.transport_codes_present
FROM inserted
JOIN packets p ON p.packet_hash = inserted.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id;

-- name: ListChannelMessages :many
-- Returns messages for a channel identified by integer ID.
-- Pass a zero/null timestamp for since to return all messages up to limit.
-- Pass empty string for iata to skip IATA filtering.
-- Pass cursor=0 to start from the beginning.
SELECT DISTINCT ON (cm.id) cm.*, encode(cm.packet_hash, 'hex') as packet_hash_hex, c.channel_hash, ts.name AS scope_name, p.transport_codes_present,
p.observation_count AS observation_count
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
SELECT DISTINCT ON (cm.id) cm.*, encode(cm.packet_hash, 'hex') as packet_hash_hex, c.channel_hash, ts.name AS scope_name, p.transport_codes_present,
p.observation_count AS observation_count
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
SELECT DISTINCT ON (cm.id) cm.*, c.channel_hash, ts.name AS scope_name, p.transport_codes_present,
  p.observation_count AS observation_count
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
SELECT DISTINCT ON (cm.id) cm.*, encode(cm.packet_hash, 'hex') as packet_hash_hex, c.channel_hash, ts.name AS scope_name, p.transport_codes_present,
p.observation_count AS observation_count
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

-- name: GetHourlyStats :many
SELECT iata, hour, observation_count
FROM analytics_hourly_iata_observations
WHERE (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0 OR iata = ANY(@iatas::bpchar[]))
  AND hour >= @since::timestamptz
ORDER BY iata, hour;

-- name: GetTopNodes :many
-- Nodes by ADVERT hearings since the given hour. Live names win over the rolled snapshot;
-- node_id is NULL once the node row is gone. iata is a representative one.
WITH ranked AS (
  SELECT origin_pubkey, SUM(observations)::bigint AS observation_count, MAX(last_heard) AS last_heard,
         MAX(iata) AS iata, MAX(name) AS name, MAX(node_type) AS node_type
  FROM analytics_hourly_advert_hearings
  WHERE hour >= @since::timestamptz
    AND (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0 OR iata = ANY(@iatas::bpchar[]))
  GROUP BY origin_pubkey
  ORDER BY observation_count DESC, origin_pubkey
  LIMIT @row_limit
)
SELECT n.id AS node_id, encode(r.origin_pubkey, 'hex') AS public_key,
       COALESCE(n.name, r.name, '')::text AS name, COALESCE(n.node_type, r.node_type, 0)::smallint AS node_type,
       r.iata::bpchar AS iata, r.observation_count, r.last_heard::timestamptz AS last_heard
FROM ranked r
LEFT JOIN nodes n ON n.public_key = r.origin_pubkey
ORDER BY r.observation_count DESC, r.origin_pubkey;

-- name: GetStatsPayloadBreakdown :many
-- Payload-type observation counts since the given hour, from the hourly rollup.
SELECT
  payload_type,
  SUM(count)::bigint AS count
FROM analytics_hourly_payload_breakdown
WHERE (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0 OR iata = ANY(@iatas::bpchar[]))
  AND hour >= @since::timestamptz
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
-- Top N observers since the given hour. Counts sum across matched IATAs; iata is a
-- representative one. Live names win over the rolled snapshot.
WITH ranked AS (
  SELECT observer_id, SUM(observation_count)::bigint AS observation_count, MAX(iata) AS iata,
         MAX(display_name) AS display_name, MAX(observer_type) AS observer_type
  FROM analytics_hourly_observer_identity
  WHERE hour >= @since::timestamptz
    AND (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0 OR iata = ANY(@iatas::bpchar[]))
  GROUP BY observer_id
  ORDER BY observation_count DESC, observer_id
  LIMIT @row_limit
)
SELECT r.observer_id AS id, COALESCE(o.display_name, r.display_name, '')::text AS display_name,
       COALESCE(o.observer_type, r.observer_type, '')::text AS observer_type,
       r.observation_count, r.iata::bpchar AS iata
FROM ranked r
LEFT JOIN observers o ON o.id = r.observer_id
ORDER BY r.observation_count DESC, r.observer_id;

-- name: GetStatsTopAdvertisers :many
-- Top N advertisers since the given hour. Each ADVERT packet counts once per hour heard,
-- however many of the requested IATAs heard it (IATA-set rollup). Live names win.
WITH ranked AS (
  SELECT origin_pubkey, SUM(advert_packets)::bigint AS advert_count,
         SUM(flood_packets)::bigint AS flood_advert_count, SUM(direct_packets)::bigint AS direct_advert_count
  FROM analytics_hourly_advert_sets
  WHERE hour >= @since::timestamptz
    AND (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0 OR iatas && @iatas::bpchar[])
  GROUP BY origin_pubkey
  ORDER BY advert_count DESC, origin_pubkey
  LIMIT @row_limit
), heard AS (
  SELECT h.origin_pubkey, MAX(h.last_heard) AS last_heard, MAX(h.iata) AS iata,
         MAX(h.name) AS name, MAX(h.node_type) AS node_type
  FROM analytics_hourly_advert_hearings h
  WHERE h.hour >= @since::timestamptz AND h.origin_pubkey IN (SELECT origin_pubkey FROM ranked)
    AND (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0 OR h.iata = ANY(@iatas::bpchar[]))
  GROUP BY h.origin_pubkey
)
SELECT n.id AS node_id, encode(r.origin_pubkey, 'hex') AS public_key,
       COALESCE(n.name, h.name, '')::text AS name, COALESCE(n.node_type, h.node_type, 0)::smallint AS node_type,
       r.advert_count, r.flood_advert_count, r.direct_advert_count,
       h.last_heard::timestamptz AS last_heard, COALESCE(h.iata, '')::bpchar AS iata
FROM ranked r
LEFT JOIN heard h ON h.origin_pubkey = r.origin_pubkey
LEFT JOIN nodes n ON n.public_key = r.origin_pubkey
ORDER BY r.advert_count DESC, r.origin_pubkey;

-- name: GetStatsClockDrift :many
-- Repeaters/room servers (node_type 2/3) whose current advert-derived clock drift exceeds
-- the given threshold in magnitude, worst first. Not time-windowed -- reflects each node's
-- latest measured drift, not an aggregate over a period.
-- Select the filtered page before aggregating its IATA memberships.
WITH page AS (
SELECT
  n.id,
  n.name,
  n.node_type,
  n.device_clock_drift_seconds,
  n.last_advert_at
FROM nodes n
WHERE n.node_type IN (2, 3)
  AND n.device_clock_drift_seconds IS NOT NULL
  AND ABS(n.device_clock_drift_seconds) > $1::int
  AND (COALESCE(cardinality($2::bpchar[]), 0) = 0 OR n.id IN (SELECT node_id FROM node_iatas WHERE iata = ANY($2::bpchar[])))
ORDER BY ABS(n.device_clock_drift_seconds) DESC
LIMIT $3
)
SELECT n.id, n.name, n.node_type, n.device_clock_drift_seconds, n.last_advert_at,
  (SELECT json_agg(json_build_object('iata', ni.iata, 'lastHeard', (extract(epoch from ni.last_heard) * 1000)::bigint) ORDER BY ni.last_heard DESC)
   FROM node_iatas ni WHERE ni.node_id = n.id) AS iatas
FROM page n
ORDER BY ABS(n.device_clock_drift_seconds) DESC;

-- name: GetStatsTopTalkers :many
-- Top N senders since the given hour. Each message counts once per hour heard, however many
-- of the requested IATAs heard it (IATA-set rollup).
SELECT
  sender_name,
  SUM(messages)::bigint AS message_count,
  MAX(last_sent)::timestamptz AS last_sent
FROM analytics_hourly_talker_sets
WHERE hour >= @since::timestamptz
  AND (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0 OR iatas && @iatas::bpchar[])
GROUP BY sender_name
ORDER BY message_count DESC, sender_name
LIMIT @row_limit;

-- name: GetRadioPresets :many
SELECT preset, iata, source_type, count
FROM mv_radio_presets
WHERE ($1::text = '' OR preset = $1::text)
  AND (COALESCE(cardinality($2::bpchar[]), 0) = 0 OR iata = ANY($2::bpchar[]))
ORDER BY preset, iata, source_type;

-- name: GetScopeStatsHourly :many
-- GetScopeStats packet counts split by hour (same window and IATA-set filter), with the
-- distinct observers and advertising nodes active in each scope that hour. Empty hours omitted.
WITH pk AS (
    SELECT s.scope_id, s.hour, SUM(s.packets)::bigint AS n
    FROM analytics_hourly_scope_sets s
    WHERE s.hour >= @since::timestamptz
      AND (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0 OR s.iatas && @iatas::bpchar[])
    GROUP BY s.scope_id, s.hour
), ob AS (
    SELECT o.scope_id, o.hour, COUNT(DISTINCT o.observer_id)::bigint AS n
    FROM analytics_hourly_scope_observers o
    WHERE o.hour >= @since::timestamptz
      AND (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0 OR o.iata = ANY(@iatas::bpchar[]))
    GROUP BY o.scope_id, o.hour
), nd AS (
    SELECT d.scope_id, d.hour, COUNT(DISTINCT d.origin_pubkey)::bigint AS n
    FROM analytics_hourly_scope_nodes d
    WHERE d.hour >= @since::timestamptz
      AND (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0 OR d.iata = ANY(@iatas::bpchar[]))
    GROUP BY d.scope_id, d.hour
), k AS (
    SELECT scope_id, hour FROM pk UNION SELECT scope_id, hour FROM ob UNION SELECT scope_id, hour FROM nd
)
SELECT ts.name, k.hour,
       COALESCE(pk.n, 0)::bigint AS packets,
       COALESCE(ob.n, 0)::bigint AS observers,
       COALESCE(nd.n, 0)::bigint AS nodes
FROM k
JOIN transport_scopes ts ON ts.id = k.scope_id
LEFT JOIN pk ON pk.scope_id = k.scope_id AND pk.hour = k.hour
LEFT JOIN ob ON ob.scope_id = k.scope_id AND ob.hour = k.hour
LEFT JOIN nd ON nd.scope_id = k.scope_id AND nd.hour = k.hour
WHERE COALESCE(pk.n, 0) > 0 OR COALESCE(ob.n, 0) > 0 OR COALESCE(nd.n, 0) > 0
ORDER BY ts.name, k.hour;

-- name: GetScopeStats :many
-- Packets since the given hour come from the IATA-set rollup (counted once per hour heard).
-- Observer and node counts are current memberships; observers filter by their latest IATA.
WITH packet_counts AS (
    SELECT s.scope_id, SUM(s.packets)::bigint AS packet_count
    FROM analytics_hourly_scope_sets s
    WHERE s.hour >= @since::timestamptz
      AND (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0 OR s.iatas && @iatas::bpchar[])
    GROUP BY s.scope_id
), observer_counts AS (
    SELECT os.scope_id, COUNT(*)::bigint AS observer_count
    FROM observer_scopes os JOIN observers o ON o.id = os.observer_id
    WHERE COALESCE(cardinality(@iatas::bpchar[]), 0) = 0 OR o.last_iata = ANY(@iatas::bpchar[])
    GROUP BY os.scope_id
), node_counts AS (
    SELECT n.default_scope_id AS scope_id, COUNT(*)::bigint AS node_count
    FROM nodes n
    WHERE n.default_scope_id IS NOT NULL
      AND (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0
        OR n.id IN (SELECT node_id FROM node_iatas WHERE iata = ANY(@iatas::bpchar[])))
    GROUP BY n.default_scope_id
)
SELECT
    ts.name,
    COALESCE(pc.packet_count, 0)::bigint AS packet_count,
    COALESCE(oc.observer_count, 0)::bigint AS observer_count,
    COALESCE(nc.node_count, 0)::bigint AS node_count
FROM transport_scopes ts
LEFT JOIN packet_counts pc ON pc.scope_id = ts.id
LEFT JOIN observer_counts oc ON oc.scope_id = ts.id
LEFT JOIN node_counts nc ON nc.scope_id = ts.id
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
    imported      = FALSE, -- config owns the slug from now on
    updated_at    = NOW()
RETURNING id;

-- name: DeleteRegionIATAsNotIn :exec
DELETE FROM region_iatas WHERE region_id = @region_id AND NOT (iata = ANY(@keep::bpchar[]));

-- name: AddRegionIATAs :exec
INSERT INTO region_iatas (region_id, iata)
SELECT @region_id, unnest(@iatas::bpchar[])
ON CONFLICT (region_id, iata) DO NOTHING;

-- name: ListRegionState :many
-- Every region with its members, for reconciling imported MeshMapper groups.
SELECT r.slug, r.name, COALESCE(r.display_order, 0)::int AS display_order, r.imported, r.center_lat, r.center_lng,
    COALESCE(array_agg(ri.iata::text ORDER BY ri.iata) FILTER (WHERE ri.iata IS NOT NULL), '{}')::text[] AS iatas
FROM regions r
LEFT JOIN region_iatas ri ON ri.region_id = r.id
GROUP BY r.id
ORDER BY r.slug;

-- name: UpsertImportedRegion :one
-- A hand-written region owns its slug: the WHERE turns a clash into no row.
INSERT INTO regions (slug, name, display_order, center_lat, center_lng, zoom_level, imported, updated_at)
VALUES (@slug, @name, @display_order, sqlc.narg(center_lat), sqlc.narg(center_lng), NULL, TRUE, NOW())
ON CONFLICT (slug) DO UPDATE SET
    name          = EXCLUDED.name,
    display_order = EXCLUDED.display_order,
    center_lat    = EXCLUDED.center_lat,
    center_lng    = EXCLUDED.center_lng,
    updated_at    = NOW()
WHERE regions.imported
RETURNING id;

-- name: PruneImportedRegions :many
DELETE FROM regions WHERE imported AND NOT (slug = ANY(@keep::text[])) RETURNING slug;

-- ============================================================
-- TRACES
-- ============================================================

-- name: ListTraceTags :many
-- Tags, most recent first. Filters match the tag summary (see RecordTrace).
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
  AND ($5::timestamptz IS NULL
       OR date_trunc('milliseconds', t.last_heard_at, 'UTC') < $5
       OR ($8::bytea IS NOT NULL AND date_trunc('milliseconds', t.last_heard_at, 'UTC') = $5 AND t.trace_tag < $8))
  AND ($7::text = '' OR t.trace_type = $7)
ORDER BY date_trunc('milliseconds', t.last_heard_at, 'UTC') DESC, t.trace_tag DESC
LIMIT $6;

-- ============================================================
-- ROUTES
-- ============================================================

-- name: UpsertKnownRoute :exec
-- Route identity is path_key, an md5 of node_ids computed by the caller.
-- On conflict, observation_count and last_seen are bumped and hash_prefix follows the
-- latest hearing, so evidence matches the hash width the route uses now.
INSERT INTO known_routes (path_key, node_ids, hash_prefix, iata, hop_count)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (iata, path_key) DO UPDATE SET
  hash_prefix = EXCLUDED.hash_prefix,
  last_seen = NOW(),
  observation_count = known_routes.observation_count + 1;

-- name: ListKnownRoutes :many
-- Only one branch runs. Keep the IATA range ordered by the composite index:
-- generic plans can otherwise prefer scanning the global timestamp index.
-- The text equality preserves exact input matching, including trailing spaces.
-- Pages by (last_seen to the ms, id) so routes sharing the cursor's millisecond
-- aren't skipped; cursor id 0 keeps the plain timestamp cursor.
SELECT r.id, r.node_ids, r.hash_prefix, r.iata, r.hop_count, r.first_seen, r.last_seen, r.observation_count
FROM ((
SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
FROM known_routes
WHERE $1 = ''
  AND ($2 = 0 OR hop_count = $2)
  AND ($3::timestamptz IS NULL
       OR date_trunc('milliseconds', last_seen, 'UTC') < $3
       OR ($5::bigint > 0 AND date_trunc('milliseconds', last_seen, 'UTC') = $3 AND id < $5))
ORDER BY date_trunc('milliseconds', last_seen, 'UTC') DESC, id DESC
LIMIT $4
)
UNION ALL
(
SELECT id, node_ids, hash_prefix, iata, hop_count, first_seen, last_seen, observation_count
FROM known_routes
WHERE $1 <> '' AND iata::text = $1
  AND iata >= $1::bpchar AND iata <= $1::bpchar
  AND ($2 = 0 OR hop_count = $2)
  AND ($3::timestamptz IS NULL
       OR date_trunc('milliseconds', last_seen, 'UTC') < $3
       OR ($5::bigint > 0 AND date_trunc('milliseconds', last_seen, 'UTC') = $3 AND id < $5))
ORDER BY iata, date_trunc('milliseconds', last_seen, 'UTC') DESC, id DESC
LIMIT $4
)) r
ORDER BY date_trunc('milliseconds', r.last_seen, 'UTC') DESC, r.id DESC
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

-- name: ResolveEndpointHashes :many
-- Logical endpoints can be any advertised role, unlike intermediate relay hops.
-- Endpoint hashes are always one byte; use the existing (iata, prefix_1) index.
-- LIMIT 1 keeps generic plans on a node PK lookup per candidate instead of
-- flattening the join into a scan of all nodes. The PK already guarantees one row.
SELECT ns.prefix_1 AS hash, n.id AS node_id, n.name, n.latitude, n.longitude, n.public_key
FROM node_short_ids ns
CROSS JOIN LATERAL (
  SELECT id, name, latitude, longitude, public_key
  FROM nodes WHERE id = ns.node_id
  LIMIT 1
) n
WHERE ns.iata = $1
  AND ns.prefix_1 = ANY($2::bytea[]);

-- name: ResolveEndpointHashPairs :many
-- Batch form of ResolveEndpointHashes for a page of packets. Matches the cross
-- product of IATAs and hashes; callers pick out the pairs they asked for.
SELECT ns.iata, ns.prefix_1 AS hash, n.id AS node_id, n.name, n.latitude, n.longitude, n.public_key
FROM node_short_ids ns
CROSS JOIN LATERAL (
  SELECT id, name, latitude, longitude, public_key
  FROM nodes WHERE id = ns.node_id
  LIMIT 1
) n
WHERE ns.iata = ANY(@iatas::bpchar[])
  AND ns.prefix_1 = ANY(@hashes::bytea[]);

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

-- name: RefreshRadioPresets :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_radio_presets;

-- name: AmbiguousPrefixes :many
-- Hop prefixes that match >1 node in an IATA, per width. Computed once per reconfirm run.
SELECT iata::text AS iata, 1::int AS len, prefix_1 AS prefix FROM node_short_ids GROUP BY iata, prefix_1 HAVING COUNT(*) > 1
UNION ALL
SELECT iata::text, 2::int, prefix_2 FROM node_short_ids GROUP BY iata, prefix_2 HAVING COUNT(*) > 1
UNION ALL
SELECT iata::text, 3::int, prefix_3 FROM node_short_ids GROUP BY iata, prefix_3 HAVING COUNT(*) > 1
UNION ALL
SELECT iata::text, 4::int, prefix_4 FROM node_short_ids GROUP BY iata, prefix_4 HAVING COUNT(*) > 1;

-- name: ReconfirmRoutes :one
-- Checks one batch of least-recently-reconfirmed routes: deletes those with a departed
-- hop node or a hop prefix now matching >1 node in that IATA (length-aware:
-- 1/2/3/4-byte hop prefixes check prefix_1/2/3/4; ambiguity set supplied by AmbiguousPrefixes),
-- and stamps the survivors.
WITH batch AS MATERIALIZED (
    SELECT r.iata, r.path_key, r.node_ids, r.hash_prefix
    FROM known_routes r
    WHERE r.last_reconfirmed_at < @before
    ORDER BY r.last_reconfirmed_at
    LIMIT @batch_size
    FOR UPDATE OF r SKIP LOCKED
),
amb AS (
    SELECT a.iata::char(3) AS iata, a.len, a.p
    FROM ROWS FROM (unnest(@amb_iata::text[]), unnest(@amb_len::int[]), unnest(@amb_prefix::bytea[])) AS a(iata, len, p)
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
),
updated AS (
    UPDATE known_routes kr
    SET last_reconfirmed_at = GREATEST(NOW(), @before::timestamptz)
    FROM batch b
    WHERE kr.iata = b.iata AND kr.path_key = b.path_key
      AND NOT EXISTS (
          SELECT 1 FROM dead d
          WHERE d.iata = b.iata AND d.path_key = b.path_key
      )
)
SELECT count(*) FROM batch;

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

-- name: CreateAccount :one
INSERT INTO accounts (name) VALUES (sqlc.arg(name))
ON CONFLICT (name) WHERE deactivated_at IS NULL DO NOTHING
RETURNING id, name, created_at, deactivated_at;

-- name: ListAccounts :many
SELECT id, name, created_at, deactivated_at FROM accounts
ORDER BY created_at DESC, id DESC;

-- name: GetAccount :one
SELECT id, name, created_at, deactivated_at FROM accounts WHERE id = $1;

-- name: DeactivateAccount :one
-- Lock the current row before deciding the outcome, including when another
-- deactivation commits while this statement is waiting for its row lock.
WITH target AS MATERIALIZED (
    SELECT a.id, a.deactivated_at FROM accounts a WHERE a.id = $1 FOR UPDATE
), changed AS (
    UPDATE accounts a SET deactivated_at = NOW()
    FROM target t WHERE a.id = t.id AND t.deactivated_at IS NULL
    RETURNING a.id
)
SELECT EXISTS(SELECT 1 FROM target) AS found,
       EXISTS(SELECT 1 FROM changed) AS deactivated;

-- name: GetObserverActivityLiveSummary :one
-- Two indexed ranges, bounded to one observer; no legacy presence counters.
WITH latest AS (
 SELECT heard_at FROM packet_observations
 WHERE observer_id = @observer_id::uuid AND heard_at <= @generated_at::timestamptz
 ORDER BY heard_at DESC LIMIT 1
), hourly AS (
 SELECT COUNT(*)::bigint AS n FROM packet_observations
 WHERE observer_id = @observer_id::uuid
 AND heard_at >= @hour_start::timestamptz AND heard_at < @hour_end::timestamptz
)
SELECT (SELECT heard_at FROM latest)::timestamptz AS latest_recorded_at,
 hourly.n AS last_complete_hour FROM hourly;

-- name: GetScopeCatalogue :one
SELECT * FROM meshmapper_scope_catalogues WHERE iata = $1 AND url = $2;

-- name: ListScopeCatalogues :many
SELECT * FROM meshmapper_scope_catalogues ORDER BY iata, attempted_at;

-- name: SaveScopeCatalogue :exec
-- One statement commits the validated snapshot and its lookup identities together.
-- Empty arrays insert nothing. NULL payload/checked_at retain last-known-good data
-- after an error or 304. Imported names never replace existing manual metadata.
WITH inserted AS (
    INSERT INTO transport_scopes (name, transport_key, key_fingerprint, imported_only)
    SELECT entry.name, entry.key, entry.fingerprint, TRUE
    FROM (SELECT unnest(@names::text[]) AS name, unnest(@keys::bytea[]) AS key,
                 unnest(@fingerprints::bytea[]) AS fingerprint) AS entry
    ON CONFLICT (name) DO NOTHING
)
INSERT INTO meshmapper_scope_catalogues (iata, url, payload, etag, checked_at, attempted_at, next_attempt, last_error)
VALUES (@iata, @url, sqlc.narg(payload)::jsonb, sqlc.narg(etag)::text,
    sqlc.narg(checked_at)::timestamptz, @attempted_at, @next_attempt, @last_error)
ON CONFLICT (iata, url) DO UPDATE SET
    payload = COALESCE(EXCLUDED.payload, meshmapper_scope_catalogues.payload),
    etag = COALESCE(EXCLUDED.etag, meshmapper_scope_catalogues.etag),
    checked_at = COALESCE(EXCLUDED.checked_at, meshmapper_scope_catalogues.checked_at),
    attempted_at = EXCLUDED.attempted_at,
    next_attempt = EXCLUDED.next_attempt,
    last_error = EXCLUDED.last_error;

-- name: ListChannelCatalogues :many
SELECT * FROM meshmapper_channel_catalogues ORDER BY iata, attempted_at;

-- name: SaveChannelCatalogue :exec
-- NULL payload/etag/checked_at retain the last good list after an error or 304.
INSERT INTO meshmapper_channel_catalogues (iata, url, payload, etag, checked_at, attempted_at, next_attempt, last_error)
VALUES (@iata, @url, sqlc.narg(payload)::jsonb, sqlc.narg(etag)::text,
    sqlc.narg(checked_at)::timestamptz, @attempted_at, @next_attempt, @last_error)
ON CONFLICT (iata, url) DO UPDATE SET
    payload = COALESCE(EXCLUDED.payload, meshmapper_channel_catalogues.payload),
    etag = COALESCE(EXCLUDED.etag, meshmapper_channel_catalogues.etag),
    checked_at = COALESCE(EXCLUDED.checked_at, meshmapper_channel_catalogues.checked_at),
    attempted_at = EXCLUDED.attempted_at,
    next_attempt = EXCLUDED.next_attempt,
    last_error = EXCLUDED.last_error;

-- name: DeleteChannelMembersNotIn :exec
DELETE FROM meshmapper_channel_members WHERE iata = @iata AND NOT (key_fingerprint = ANY(@keep::bytea[]));

-- name: AddChannelMembers :exec
INSERT INTO meshmapper_channel_members (iata, key_fingerprint)
SELECT @iata, unnest(@fingerprints::bytea[])
ON CONFLICT (iata, key_fingerprint) DO NOTHING;

-- name: DeleteAllChannelMembers :exec
DELETE FROM meshmapper_channel_members;

-- name: ListZoneBoundaries :many
SELECT * FROM meshmapper_zone_boundaries ORDER BY iata;

-- name: SaveZoneBoundary :exec
-- NULL feature/etag/checked_at retain the last good boundary after an error or 304.
INSERT INTO meshmapper_zone_boundaries (iata, url, feature, etag, checked_at, attempted_at, next_attempt, last_error)
VALUES (@iata, @url, sqlc.narg(feature)::jsonb, sqlc.narg(etag)::text,
    sqlc.narg(checked_at)::timestamptz, @attempted_at, @next_attempt, @last_error)
ON CONFLICT (iata) DO UPDATE SET
    url = EXCLUDED.url,
    feature = COALESCE(EXCLUDED.feature, meshmapper_zone_boundaries.feature),
    etag = COALESCE(EXCLUDED.etag, meshmapper_zone_boundaries.etag),
    checked_at = COALESCE(EXCLUDED.checked_at, meshmapper_zone_boundaries.checked_at),
    attempted_at = EXCLUDED.attempted_at,
    next_attempt = EXCLUDED.next_attempt,
    last_error = EXCLUDED.last_error;

-- name: ListZoneLists :many
SELECT * FROM meshmapper_zone_lists ORDER BY country;

-- name: SaveZoneList :exec
-- NULL payload/etag/fetched_at retain the last good list after an error or 304.
INSERT INTO meshmapper_zone_lists (country, payload, etag, fetched_at, attempted_at, next_attempt, last_error)
VALUES (@country, sqlc.narg(payload)::jsonb, sqlc.narg(etag)::text,
    sqlc.narg(fetched_at)::timestamptz, @attempted_at, @next_attempt, @last_error)
ON CONFLICT (country) DO UPDATE SET
    payload = COALESCE(EXCLUDED.payload, meshmapper_zone_lists.payload),
    etag = COALESCE(EXCLUDED.etag, meshmapper_zone_lists.etag),
    fetched_at = COALESCE(EXCLUDED.fetched_at, meshmapper_zone_lists.fetched_at),
    attempted_at = EXCLUDED.attempted_at,
    next_attempt = EXCLUDED.next_attempt,
    last_error = EXCLUDED.last_error;

-- name: PruneZoneBoundaries :many
-- Drops imports for IATAs no longer configured, so their manual border returns.
DELETE FROM meshmapper_zone_boundaries WHERE NOT (iata = ANY(@keep::text[])) RETURNING iata;
