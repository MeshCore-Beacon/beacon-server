-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Typed region pages walk one range per (site, payload type, route type).
CREATE INDEX CONCURRENTLY idx_packet_sites_site_type ON packet_sites USING btree (iata, payload_type, route_type, site_heard_at DESC, packet_hash DESC);
