-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- All-channel pages filtered by site walk one site range each.
CREATE INDEX CONCURRENTLY idx_cmi_site ON channel_message_iatas USING btree (iata, message_id DESC);
