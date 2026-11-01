-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- All-channel pages filtered by scope alone.
CREATE INDEX CONCURRENTLY idx_cmi_scope ON channel_message_iatas USING btree (scope_id, message_id DESC) WHERE (scope_id IS NOT NULL);
