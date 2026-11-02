-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Unfiltered channel pages walk the channel by id, the cursor's own column.
CREATE INDEX CONCURRENTLY idx_channel_messages_channel_id ON channel_messages USING btree (channel_id, id DESC);
