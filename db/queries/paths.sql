-- name: GetPathStats :many
-- Header contents per reception, not a complete traversed route. TRACE paths
-- contain SNR bytes. Missing legacy payload types cannot safely vote for widths.
-- Keep bpchar index access in both custom and generic prepared plans.
WITH readings AS (
    SELECT heard_at, payload_type, path_length_byte, hash_size, hop_count, path_bytes
    FROM packet_observations
    WHERE heard_at >= @since::timestamptz AND heard_at < @until::timestamptz
      AND COALESCE(cardinality(@iatas::bpchar[]), 0) = 0
    UNION ALL
    SELECT heard_at, payload_type, path_length_byte, hash_size, hop_count, path_bytes
    FROM packet_observations
    WHERE heard_at >= @since::timestamptz AND heard_at < @until::timestamptz
      AND cardinality(@iatas::bpchar[]) > 0 AND iata = ANY(@iatas::bpchar[])
), classified AS (
    SELECT heard_at, hash_size, hop_count,
           CASE WHEN payload_type = 9 THEN 2
                -- Match meshcore-go IsValidPathLen (1/2/3-byte hashes, max 64 path bytes).
                WHEN payload_type IS NULL OR payload_type NOT BETWEEN 0 AND 15
                  OR NOT (path_length_byte BETWEEN 0 AND 191
                    AND hash_size BETWEEN 1 AND 3 AND hop_count BETWEEN 0 AND 63
                    AND hash_size = (path_length_byte >> 6) + 1
                    AND hop_count = (path_length_byte & 63)
                    AND hash_size::integer * hop_count::integer <= 64
                    AND COALESCE(octet_length(path_bytes), 0) = hash_size::integer * hop_count::integer)
                THEN 3
                WHEN hop_count = 0 THEN 1
                ELSE 0 END::integer AS category
    FROM readings
), buckets AS (
    SELECT date_trunc('hour', heard_at, 'UTC') AS hour, category,
           CASE WHEN category = 0 THEN hash_size ELSE 0 END::integer AS hash_bytes,
           CASE WHEN category = 0 THEN hop_count ELSE 0 END::integer AS entries
    FROM classified
)
SELECT (grouping(hour) = 0)::boolean AS is_hourly,
       COALESCE(hour, 'epoch'::timestamptz)::timestamptz AS hour,
       category, hash_bytes, COALESCE(entries, 0)::integer AS entries,
       count(*)::bigint AS receptions
FROM buckets
GROUP BY GROUPING SETS ((category, hash_bytes, entries), (hour, category, hash_bytes))
ORDER BY is_hourly, hour, category, hash_bytes, entries;
