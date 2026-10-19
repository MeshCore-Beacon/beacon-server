-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: InsertObservation :one
WITH ins AS (
  INSERT INTO packet_observations (
    packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count, path_bytes, rssi, snr,
    propagation_time_ms, radio_freq_mhz, spread_factor, bandwidth_khz, coding_rate, source_broker, payload_type, airtime_ms
  ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
  ON CONFLICT (packet_hash, observer_id) DO NOTHING
  RETURNING packet_hash, iata, heard_at
), bump AS (
  UPDATE packets p SET observation_count = p.observation_count + 1 FROM ins WHERE p.packet_hash = ins.packet_hash
  RETURNING p.packet_hash, p.observation_count, p.payload_type, p.route_type, p.scope_id, p.first_heard_at
), site AS (
  INSERT INTO packet_sites (packet_hash, iata, site_heard_at, first_heard_at, payload_type, route_type, scope_id)
  SELECT b.packet_hash, i.iata, i.heard_at, b.first_heard_at, b.payload_type, b.route_type, b.scope_id
  FROM ins i JOIN bump b USING (packet_hash)
  ON CONFLICT (packet_hash, iata) DO UPDATE SET site_heard_at = EXCLUDED.site_heard_at
     WHERE packet_sites.site_heard_at < EXCLUDED.site_heard_at
)
SELECT EXISTS (SELECT 1 FROM ins) AS inserted,
       COALESCE((SELECT b.observation_count FROM bump b),
                (SELECT pk.observation_count FROM packets pk WHERE pk.packet_hash = $1), 0)::bigint AS observation_count
