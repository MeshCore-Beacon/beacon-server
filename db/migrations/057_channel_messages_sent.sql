-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Message pages across every channel with sites and since count, then read, the messages sent since then; sender
-- clocks put sent_at anywhere, so the id order does not bound them.
CREATE INDEX CONCURRENTLY idx_channel_messages_sent ON channel_messages USING btree (sent_at);
