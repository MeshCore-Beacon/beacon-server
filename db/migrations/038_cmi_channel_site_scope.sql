-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Channel pages filtered by site and scope.
CREATE INDEX CONCURRENTLY idx_cmi_channel_site_scope ON channel_message_iatas USING btree (channel_id, iata, scope_id, message_id DESC) WHERE (scope_id IS NOT NULL);
