-- name: GetSignalStats :many
-- One reception scan supplies totals, hourly means, and two independent histograms.
-- The zero/zero pair is Beacon's existing unavailable-reading sentinel. A real
-- zero SNR with nonzero RSSI remains valid. Non-finite SNR never reaches JSON.
WITH samples AS (
    SELECT heard_at,
           CASE WHEN NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0)
                      AND snr > '-Infinity'::real AND snr < 'Infinity'::real
                THEN snr::double precision END AS snr,
           CASE WHEN NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0)
                THEN rssi::double precision END AS rssi
    FROM packet_observations
    WHERE heard_at >= @since::timestamptz AND heard_at < @until::timestamptz
      AND (COALESCE(cardinality(@iatas::text[]), 0) = 0 OR iata = ANY(@iatas::text[]))
), binned AS (
    SELECT date_trunc('hour', heard_at, 'UTC') AS hour, snr, rssi,
           width_bucket(snr, -30, 30, 12) AS snr_bin,
           width_bucket(rssi, -140, 0, 14) AS rssi_bin
    FROM samples
)
SELECT grouping(hour, snr_bin, rssi_bin)::integer AS kind,
       COALESCE(hour, 'epoch'::timestamptz)::timestamptz AS hour,
       COALESCE(snr_bin, -1)::integer AS snr_bin,
       COALESCE(rssi_bin, -1)::integer AS rssi_bin,
       count(*)::bigint AS receptions,
       count(snr)::bigint AS snr_samples,
       COALESCE(avg(snr), 0)::double precision AS snr_average,
       count(rssi)::bigint AS rssi_samples,
       COALESCE(avg(rssi), 0)::double precision AS rssi_average
FROM binned
GROUP BY GROUPING SETS ((), (hour), (snr_bin), (rssi_bin))
ORDER BY kind, hour, snr_bin, rssi_bin;
