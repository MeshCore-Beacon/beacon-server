-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- The packet_hash FK had no ON DELETE action, so packets carrying a channel
-- message could never age out and DeleteOldPackets aborted. Match packet_observations.

ALTER TABLE channel_messages
  DROP CONSTRAINT IF EXISTS channel_messages_packet_hash_fkey;

ALTER TABLE channel_messages
  ADD CONSTRAINT channel_messages_packet_hash_fkey
  FOREIGN KEY (packet_hash) REFERENCES packets(packet_hash) ON DELETE CASCADE;
