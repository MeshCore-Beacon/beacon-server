-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: DeleteOldPacketsVictims
-- The read-only part of DeleteOldPackets: its victims CTE without the lock clause.
-- Parameters as in DeleteOldPackets: $1 cutoff, $2 limit.
SELECT vp.packet_hash, vp.last_heard_at FROM packets vp
  WHERE vp.last_heard_at < $1::timestamptz
  ORDER BY vp.last_heard_at, vp.packet_hash
  LIMIT $2::integer
