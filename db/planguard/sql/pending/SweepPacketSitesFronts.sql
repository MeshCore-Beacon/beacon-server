-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: SweepPacketSitesFronts :one
-- maint, every 30 s; run with SET LOCAL enable_bitmapscan = off, enable_seqscan = off.
-- Read-only. Visits, through each time-ordered packet_sites index, every entry whose key was written in the last
-- @lookback (2 minutes). An index scan that reaches an entry whose heap tuple is dead to every snapshot sets the
-- entry's LP_DEAD hint; when that leaf later fills, the inserting backend deletes hinted entries instead of splitting
-- (nbtree simple deletion). Keeps the three indexes at ~2x a fresh build instead of 4-9x. Changes no row.
-- The ORDER BY ... LIMIT keeps every lateral a per-front index range scan (never flattened into a join).
-- The cap is far above any front's 2-minute volume (SEA: ~150 entries) and only bounds the gauge.
WITH site AS MATERIALIZED (
  SELECT ps.iata, ps.payload_type, ps.route_type, ps.scope_id
  FROM iata_codes c
  CROSS JOIN LATERAL (
    SELECT p.iata, p.payload_type, p.route_type, p.scope_id
    FROM packet_sites p
    WHERE p.iata = c.iata AND p.site_heard_at > now() - $1::interval
    ORDER BY p.site_heard_at DESC LIMIT 100000) ps                   -- idx_packet_sites_site, Index Scan
), typ AS (
  SELECT count(*) AS n
  FROM (SELECT DISTINCT iata, payload_type, route_type FROM site) f
  CROSS JOIN LATERAL (
    SELECT 1 FROM packet_sites p
    WHERE p.iata = f.iata AND p.payload_type = f.payload_type AND p.route_type = f.route_type
      AND p.site_heard_at > now() - $1::interval
    ORDER BY p.site_heard_at DESC LIMIT 100000) x                    -- idx_packet_sites_site_type, Index Only Scan
), scp AS (
  SELECT count(*) AS n
  FROM (SELECT DISTINCT iata, scope_id FROM site WHERE scope_id IS NOT NULL) f
  CROSS JOIN LATERAL (
    SELECT 1 FROM packet_sites p
    WHERE p.iata = f.iata AND p.scope_id = f.scope_id AND p.scope_id IS NOT NULL
      AND p.site_heard_at > now() - $1::interval
    ORDER BY p.site_heard_at DESC LIMIT 100000) x                    -- idx_packet_sites_site_scope, Index Only Scan
)
SELECT (SELECT count(*) FROM site) AS site_live, (SELECT n FROM typ) AS type_live, (SELECT n FROM scp) AS scope_live;
