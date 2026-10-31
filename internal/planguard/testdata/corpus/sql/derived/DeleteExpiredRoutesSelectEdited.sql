-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later
-- The read-only part of DeleteExpiredRoutes: its expired CTE without the lock clause.
SELECT r.iata, r.path_key, date_trunc('milliseconds', r.last_seen, 'UTC') AS ts, r.id
  FROM known_routes r
  WHERE (date_trunc('milliseconds', r.last_seen, 'UTC'), r.id) > ($1::timestamptz, $2::bigint)
    AND date_trunc('milliseconds', r.last_seen, 'UTC') <= $3::timestamptz
    AND r.last_seen + interval '0' < $4::timestamptz
  ORDER BY date_trunc('milliseconds', r.last_seen, 'UTC'), r.id
  LIMIT $5
