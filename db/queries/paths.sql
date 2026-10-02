-- name: GetPathStats :many
-- All HTTP aggregates read the hourly rollup, never observations.
WITH readings AS (
    SELECT iata, hour, category, hash_bytes, entries, receptions FROM analytics_hourly_paths
    WHERE hour >= @since::timestamptz AND hour < @until::timestamptz
      AND COALESCE(cardinality(@iatas::bpchar[]), 0) = 0
    UNION ALL
    SELECT iata, hour, category, hash_bytes, entries, receptions FROM analytics_hourly_paths
    WHERE hour >= @since::timestamptz AND hour < @until::timestamptz
      AND cardinality(@iatas::bpchar[]) > 0 AND iata = ANY(@iatas::bpchar[])
)
SELECT (grouping(hour) = 0)::boolean AS is_hourly,
       COALESCE(hour, 'epoch'::timestamptz)::timestamptz AS hour,
       category, hash_bytes, COALESCE(entries, 0)::integer AS entries,
       sum(receptions)::bigint AS receptions,
       -- Non-routed categories store entries = 0, so an hour of only empty paths gives 0.
       COALESCE(max(entries) FILTER (WHERE receptions > 0), 0)::integer AS max_entries
FROM readings
GROUP BY GROUPING SETS ((category, hash_bytes, entries), (hour, category, hash_bytes))
ORDER BY is_hourly, hour, category, hash_bytes, entries;

