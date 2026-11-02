-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Region pages with until read each site from until + 30 minutes down; a packet heard at a site more than 30 minutes
-- after it was first heard is the only kind that can sit above that and still be first heard by until.
CREATE INDEX CONCURRENTLY idx_packet_sites_late ON packet_sites USING btree (iata, site_heard_at DESC, packet_hash DESC) INCLUDE (first_heard_at, payload_type, route_type, scope_id) WHERE ((site_heard_at - first_heard_at) > '00:30:00'::interval);
