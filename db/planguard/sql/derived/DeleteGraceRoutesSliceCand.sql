-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: DeleteGraceRoutesSliceCand
-- The read-only part of DeleteGraceRoutesSlice: its cand CTE (it holds no lock clause).
-- Parameters as in DeleteGraceRoutesSlice: $1 grace cutoff (ms ceiling), $2 after_ts, $3 after_id, $4 grace cutoff,
-- $5 min_observations, $6 limit.
SELECT r.iata, r.path_key, date_trunc('milliseconds', r.last_seen, 'UTC') AS ts, r.id
  FROM known_routes r
  WHERE (date_trunc('milliseconds', r.last_seen, 'UTC'), r.id) > ($2::timestamptz, $3::bigint)
    AND date_trunc('milliseconds', r.last_seen, 'UTC')
          < LEAST($1::timestamptz, $2::timestamptz + interval '6 hours')
    AND r.last_seen + interval '0' < $4::timestamptz
    AND r.observation_count < $5::bigint
  ORDER BY date_trunc('milliseconds', r.last_seen, 'UTC'), r.id
  LIMIT $6
