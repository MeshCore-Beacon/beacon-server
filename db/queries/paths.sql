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

-- name: GetPathStatsProbe :many
-- GetPathStats for at most 32 distinct IATAs: one rollup probe per (hour, IATA). @since and @until are UTC hours.
-- The hours walked stay inside the table's own [min(hour), max(hour)]; an empty table walks none
-- (a NULL start makes generate_series return no rows).
WITH readings AS (
    SELECT iata, hour, category, hash_bytes, entries, receptions FROM analytics_hourly_paths
    WHERE hour >= @since::timestamptz AND hour < @until::timestamptz
      AND COALESCE(cardinality(@iatas::bpchar[]), 0) = 0
    UNION ALL
    SELECT p.iata, p.hour, p.category, p.hash_bytes, p.entries, p.receptions
    FROM generate_series((SELECT CASE WHEN min(hour) IS NOT NULL THEN GREATEST(@since::timestamptz, min(hour)) END FROM analytics_hourly_paths),
                         (SELECT LEAST(@until::timestamptz, max(hour)) FROM analytics_hourly_paths), interval '1 hour') g(h)
    CROSS JOIN (SELECT DISTINCT unnest(@iatas::bpchar[]) AS iata) i
    CROSS JOIN LATERAL (SELECT x.iata, x.hour, x.category, x.hash_bytes, x.entries, x.receptions
                        FROM analytics_hourly_paths x WHERE x.hour = g.h AND x.iata = i.iata OFFSET 0) p
    WHERE g.h < @until::timestamptz AND cardinality(@iatas::bpchar[]) > 0
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
