-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Scoped region pages walk one range per (site, scope).
CREATE INDEX CONCURRENTLY idx_packet_sites_site_scope ON packet_sites USING btree (iata, scope_id, site_heard_at DESC, packet_hash DESC) INCLUDE (payload_type, route_type, first_heard_at) WHERE (scope_id IS NOT NULL);
