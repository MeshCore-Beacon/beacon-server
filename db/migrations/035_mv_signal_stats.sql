-- Reception-IATA rollups include legacy NULL payload types and exclude non-finite
-- SNR. mv_observer_activity_hourly has a different grain/filter, so reusing its
-- means would disagree with these histograms and the reception coverage counts.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_signal_stats_hourly AS
WITH samples AS (
    SELECT iata, date_trunc('hour', heard_at, 'UTC') AS hour,
           CASE WHEN NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0)
                     AND snr > '-Infinity'::real AND snr < 'Infinity'::real
                THEN snr::double precision END AS snr,
           CASE WHEN NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0)
                THEN rssi::double precision END AS rssi
    FROM packet_observations
    WHERE heard_at >= date_trunc('hour', NOW(), 'UTC') - INTERVAL '30 days'
), binned AS (
    SELECT *, width_bucket(snr, -30, 30, 12) AS snr_bin,
              width_bucket(rssi, -140, 0, 14) AS rssi_bin
    FROM samples
)
SELECT iata, hour, grouping(snr_bin, rssi_bin)::integer AS kind,
       COALESCE(snr_bin, -1)::integer AS snr_bin,
       COALESCE(rssi_bin, -1)::integer AS rssi_bin,
       count(*)::bigint AS receptions,
       count(snr)::bigint AS snr_samples,
       COALESCE(sum(snr), 0)::double precision AS snr_sum,
       count(rssi)::bigint AS rssi_samples,
       COALESCE(sum(rssi), 0)::double precision AS rssi_sum
FROM binned
GROUP BY GROUPING SETS ((iata, hour), (iata, hour, snr_bin), (iata, hour, rssi_bin));

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_signal_stats_hourly
    ON mv_signal_stats_hourly (iata, hour, kind, snr_bin, rssi_bin);
CREATE INDEX IF NOT EXISTS idx_mv_signal_stats_hourly_hour ON mv_signal_stats_hourly (hour);
