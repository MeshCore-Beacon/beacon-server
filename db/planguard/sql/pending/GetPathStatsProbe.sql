-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: GetPathStatsProbe
WITH readings AS (
    SELECT iata, hour, category, hash_bytes, entries, receptions FROM analytics_hourly_paths
    WHERE hour >= $1::timestamptz AND hour < $2::timestamptz
      AND COALESCE(cardinality($3::bpchar[]), 0) = 0
    UNION ALL
    SELECT p.iata, p.hour, p.category, p.hash_bytes, p.entries, p.receptions
    FROM generate_series(GREATEST($1::timestamptz, (SELECT min(hour) FROM analytics_hourly_paths)), $2::timestamptz, interval '1 hour') g(h)
    CROSS JOIN (SELECT DISTINCT unnest($3::bpchar[]) AS iata) i
    CROSS JOIN LATERAL (SELECT x.iata, x.hour, x.category, x.hash_bytes, x.entries, x.receptions
                        FROM analytics_hourly_paths x WHERE x.hour = g.h AND x.iata = i.iata OFFSET 0) p
    WHERE g.h < $2::timestamptz AND cardinality($3::bpchar[]) > 0
)
SELECT (grouping(hour) = 0)::boolean AS is_hourly,
       COALESCE(hour, 'epoch'::timestamptz)::timestamptz AS hour,
       category, hash_bytes, COALESCE(entries, 0)::integer AS entries,
       sum(receptions)::bigint AS receptions,
       -- Non-routed categories store entries = 0, so an hour of only empty paths gives 0.
       COALESCE(max(entries) FILTER (WHERE receptions > 0), 0)::integer AS max_entries
FROM readings
GROUP BY GROUPING SETS ((category, hash_bytes, entries), (hour, category, hash_bytes))
ORDER BY is_hourly, hour, category, hash_bytes, entries
