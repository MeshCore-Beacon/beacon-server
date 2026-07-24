-- Precomputed observer activity, bucketed by hour so top-observers can be
-- served for any window (24h/7d/30d) by summing the buckets in range. Was a
-- ~2s per-request scan of a week of observations.

CREATE MATERIALIZED VIEW mv_top_observers_by_iata AS
SELECT
  po.iata,
  po.observer_id,
  o.display_name,
  o.observer_type,
  date_trunc('hour', po.heard_at)::timestamptz AS bucket,
  COUNT(*) AS observation_count
FROM packet_observations po
JOIN observers o ON o.id = po.observer_id
WHERE po.heard_at > NOW() - INTERVAL '30 days'
GROUP BY po.iata, po.observer_id, o.display_name, o.observer_type, date_trunc('hour', po.heard_at);

CREATE UNIQUE INDEX idx_mv_top_observers
  ON mv_top_observers_by_iata(iata, observer_id, bucket);
