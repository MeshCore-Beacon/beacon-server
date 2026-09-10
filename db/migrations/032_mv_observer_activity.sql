-- Hourly per-observer rollup so 7d/30d activity sums a few thousand rows instead of scanning
-- a month of observations. payload_type is a key so the breakdown comes from the same rows.
-- sqlc types the cast aggregates as non-null even though airtime_ms/snr_*/rssi_sum are NULL for
-- empty groups, so readers must gate on the matching *_n counts.
CREATE MATERIALIZED VIEW mv_observer_activity_hourly AS
SELECT
  observer_id,
  payload_type,
  date_trunc('hour', heard_at)::timestamptz AS bucket,
  COUNT(*)::bigint AS observations,
  SUM(airtime_ms)::real AS airtime_ms,
  COUNT(airtime_ms)::bigint AS airtime_n,
  SUM(snr)   FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::real   AS snr_sum,
  COUNT(snr) FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::bigint AS snr_n,
  MIN(snr)   FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::real   AS snr_min,
  SUM(rssi)  FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::bigint AS rssi_sum,
  COUNT(rssi) FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::bigint AS rssi_n
FROM packet_observations
WHERE heard_at > NOW() - INTERVAL '30 days'
  AND payload_type IS NOT NULL
GROUP BY observer_id, payload_type, date_trunc('hour', heard_at);

CREATE UNIQUE INDEX idx_mv_observer_activity_hourly
  ON mv_observer_activity_hourly(observer_id, payload_type, bucket);
