-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Scoped region pages with a type filter walk one range per (site, scope, payload type, route type): neither the
-- scope range nor the type range bounds a rare scope and a rare type together.
CREATE INDEX CONCURRENTLY idx_packet_sites_site_scope_type ON packet_sites USING btree (iata, scope_id, payload_type, route_type, site_heard_at DESC, packet_hash DESC) INCLUDE (first_heard_at) WHERE (scope_id IS NOT NULL);
