-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Region pages walk one (site_heard_at, packet_hash) range per site; the included columns keep window filters index-only.
CREATE INDEX CONCURRENTLY idx_packet_sites_site ON packet_sites USING btree (iata, site_heard_at DESC, packet_hash DESC) INCLUDE (payload_type, route_type, first_heard_at);
