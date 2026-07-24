-- Hourly-bucketed advert counts (distinct ADVERT packets per node = adverts sent,
-- not heard) so top-advertisers serves any window from a small table, not the live
-- scan. Per-IATA counts; cross-IATA adverts are approximate all-regions.

CREATE MATERIALIZED VIEW mv_top_advertisers_by_iata AS
SELECT
  po.iata,
  n.id AS node_id,
  n.name,
  n.node_type,
  date_trunc('hour', po.heard_at)::timestamptz AS bucket,
  COUNT(DISTINCT p.packet_hash) AS advert_count,
  MAX(po.heard_at) AS last_heard
FROM packets p
JOIN packet_observations po ON po.packet_hash = p.packet_hash
JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE p.payload_type = 4 -- ADVERT
  AND po.heard_at > NOW() - INTERVAL '30 days'
GROUP BY po.iata, n.id, n.name, n.node_type, date_trunc('hour', po.heard_at);

CREATE UNIQUE INDEX idx_mv_top_advertisers
  ON mv_top_advertisers_by_iata(iata, node_id, bucket);
