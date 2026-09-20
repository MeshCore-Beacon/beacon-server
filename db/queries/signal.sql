-- name: GetSignalStats :many
-- Read compact hourly snapshots, never observations on an HTTP request.
-- Weight averages by sample counts instead of averaging regional/hourly means.
WITH readings AS (
    SELECT * FROM mv_signal_stats_hourly
    WHERE hour >= @since::timestamptz AND hour < @until::timestamptz
      AND COALESCE(cardinality(@iatas::bpchar[]), 0) = 0
    UNION ALL
    SELECT * FROM mv_signal_stats_hourly
    WHERE hour >= @since::timestamptz AND hour < @until::timestamptz
      AND cardinality(@iatas::bpchar[]) > 0 AND iata = ANY(@iatas::bpchar[])
)
SELECT (3 + 4 * grouping(hour))::integer AS kind,
       COALESCE(hour, 'epoch'::timestamptz)::timestamptz AS hour,
       -1::integer AS snr_bin, -1::integer AS rssi_bin,
       COALESCE(sum(receptions), 0)::bigint AS receptions,
       COALESCE(sum(snr_samples), 0)::bigint AS snr_samples,
       COALESCE(sum(snr_sum) / NULLIF(sum(snr_samples), 0), 0)::double precision AS snr_average,
       COALESCE(sum(rssi_samples), 0)::bigint AS rssi_samples,
       COALESCE(sum(rssi_sum) / NULLIF(sum(rssi_samples), 0), 0)::double precision AS rssi_average
FROM readings WHERE kind = 3
GROUP BY GROUPING SETS ((), (hour))
UNION ALL
SELECT (kind + 4)::integer, 'epoch'::timestamptz, snr_bin, rssi_bin,
       sum(receptions)::bigint, 0::bigint, 0::double precision, 0::bigint, 0::double precision
FROM readings WHERE kind IN (1, 2)
GROUP BY kind, snr_bin, rssi_bin
ORDER BY kind, hour, snr_bin, rssi_bin;

-- name: RefreshSignalStats :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_signal_stats_hourly;
