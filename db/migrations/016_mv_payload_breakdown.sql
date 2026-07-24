-- Precomputed payload breakdown so the request reads a small table instead of
-- scanning a week of observations.

CREATE MATERIALIZED VIEW mv_payload_breakdown_by_iata AS
SELECT
  iata,
  payload_type,
  COUNT(*) AS count
FROM packet_observations
WHERE heard_at > NOW() - INTERVAL '7 days'
  AND payload_type IS NOT NULL
GROUP BY iata, payload_type;

CREATE UNIQUE INDEX idx_mv_payload_breakdown
  ON mv_payload_breakdown_by_iata(iata, payload_type);
