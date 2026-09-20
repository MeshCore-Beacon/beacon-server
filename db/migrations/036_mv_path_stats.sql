-- Classify once per background refresh, preserving the decoder's header rules.
-- No observation rows or deduplication constraints are changed.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_path_stats_hourly AS
WITH classified AS (
    SELECT iata, heard_at, hash_size, hop_count,
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
    FROM packet_observations
    WHERE heard_at >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
), buckets AS (
    SELECT iata, date_trunc('hour', heard_at, 'UTC') AS hour, category,
           CASE WHEN category = 0 THEN hash_size ELSE 0 END::integer AS hash_bytes,
           CASE WHEN category = 0 THEN hop_count ELSE 0 END::integer AS entries
    FROM classified
)
SELECT iata, hour, category, hash_bytes, entries, count(*)::bigint AS receptions
FROM buckets GROUP BY iata, hour, category, hash_bytes, entries;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_path_stats_hourly
    ON mv_path_stats_hourly (iata, hour, category, hash_bytes, entries);
CREATE INDEX IF NOT EXISTS idx_mv_path_stats_hourly_hour ON mv_path_stats_hourly (hour);
