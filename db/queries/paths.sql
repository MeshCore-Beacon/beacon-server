-- name: GetPathStats :many
-- All HTTP aggregates read the compact classification snapshot.
WITH readings AS (
    SELECT * FROM mv_path_stats_hourly
    WHERE hour >= @since::timestamptz AND hour < @until::timestamptz
      AND COALESCE(cardinality(@iatas::bpchar[]), 0) = 0
    UNION ALL
    SELECT * FROM mv_path_stats_hourly
    WHERE hour >= @since::timestamptz AND hour < @until::timestamptz
      AND cardinality(@iatas::bpchar[]) > 0 AND iata = ANY(@iatas::bpchar[])
)
SELECT (grouping(hour) = 0)::boolean AS is_hourly,
       COALESCE(hour, 'epoch'::timestamptz)::timestamptz AS hour,
       category, hash_bytes, COALESCE(entries, 0)::integer AS entries,
       sum(receptions)::bigint AS receptions
FROM readings
GROUP BY GROUPING SETS ((category, hash_bytes, entries), (hour, category, hash_bytes))
ORDER BY is_hourly, hour, category, hash_bytes, entries;

-- name: RefreshPathStats :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_path_stats_hourly;
