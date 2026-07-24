-- Precomputed payload breakdown, bucketed by hour so the stats endpoint can
-- serve any window (24h/7d/30d) by summing the buckets in range instead of
-- scanning a month of observations per request.

CREATE MATERIALIZED VIEW mv_payload_breakdown_by_iata AS
SELECT
  iata,
  payload_type,
  date_trunc('hour', heard_at)::timestamptz AS bucket,
  COUNT(*) AS count
FROM packet_observations
WHERE heard_at > NOW() - INTERVAL '30 days'
  AND payload_type IS NOT NULL
GROUP BY iata, payload_type, date_trunc('hour', heard_at);

CREATE UNIQUE INDEX idx_mv_payload_breakdown
  ON mv_payload_breakdown_by_iata(iata, payload_type, bucket);
