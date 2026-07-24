-- Precomputed top observers, same shape as mv_top_nodes_by_iata (was a ~2s
-- per-request scan of a week of observations).

CREATE MATERIALIZED VIEW mv_top_observers_by_iata AS
SELECT
  po.iata,
  po.observer_id,
  o.display_name,
  o.observer_type,
  COUNT(*) AS observation_count,
  MAX(po.heard_at) AS last_heard
FROM packet_observations po
JOIN observers o ON o.id = po.observer_id
WHERE po.heard_at > NOW() - INTERVAL '7 days'
GROUP BY po.iata, po.observer_id, o.display_name, o.observer_type;

CREATE UNIQUE INDEX idx_mv_top_observers
  ON mv_top_observers_by_iata(iata, observer_id);
