-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Region pages with since stop each site at since - 30 minutes; a packet heard at a site more than 30 minutes before
-- it was first heard is the only kind below that which can still be first heard at or after since. Ingest clamps
-- hearing times to 30 minutes of its clock, so this index stays about empty.
CREATE INDEX CONCURRENTLY idx_packet_sites_early ON packet_sites USING btree (iata, first_heard_at) INCLUDE (site_heard_at, packet_hash, payload_type, route_type, scope_id) WHERE ((first_heard_at - site_heard_at) > '00:30:00'::interval);
