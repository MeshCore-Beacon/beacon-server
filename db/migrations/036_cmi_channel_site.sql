-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Channel pages filtered by site walk one (channel, site) range each.
CREATE INDEX CONCURRENTLY idx_cmi_channel_site ON channel_message_iatas USING btree (channel_id, iata, message_id DESC);
