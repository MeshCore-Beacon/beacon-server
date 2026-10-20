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
