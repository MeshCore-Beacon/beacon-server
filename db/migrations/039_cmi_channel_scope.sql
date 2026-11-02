-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Channel pages filtered by scope alone: rare and absent scopes read a few entries instead of the channel.
CREATE INDEX CONCURRENTLY idx_cmi_channel_scope ON channel_message_iatas USING btree (channel_id, scope_id, message_id DESC) WHERE (scope_id IS NOT NULL);
