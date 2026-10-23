-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Fixture step 66: packet_sites, as the backfill fills it: one row per packet and site at its newest hearing there,
-- in ingest order (first observation id).
INSERT INTO packet_sites (packet_hash, iata, site_heard_at, first_heard_at, payload_type, route_type, scope_id)
SELECT po.packet_hash, po.iata, max(po.heard_at), p.first_heard_at, p.payload_type, p.route_type, p.scope_id
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
GROUP BY po.packet_hash, po.iata, p.first_heard_at, p.payload_type, p.route_type, p.scope_id
ORDER BY min(po.id);
