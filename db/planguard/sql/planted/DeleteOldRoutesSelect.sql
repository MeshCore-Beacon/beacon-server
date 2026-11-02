-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: DeleteOldRoutesSelect
-- The read-only part of DeleteOldRoutes as of 202acc6, with its row lock (plain EXPLAIN only).
    SELECT r.iata, r.path_key
    FROM known_routes r
    WHERE r.last_seen < GREATEST($1::timestamptz, $2::timestamptz)
      AND (r.last_seen < $1 OR
           (r.observation_count < $3 AND r.last_seen < $2))
    LIMIT $4
    FOR UPDATE OF r SKIP LOCKED
