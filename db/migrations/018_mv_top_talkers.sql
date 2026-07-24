-- Hourly-bucketed talker counts so top-talkers serves any window from a small
-- table instead of the live channel_messages/observations join (millions of rows
-- per request). Per-IATA counts; cross-IATA senders are approximate all-regions.

CREATE MATERIALIZED VIEW mv_top_talkers_by_iata AS
SELECT
  po.iata,
  cm.sender_name,
  date_trunc('hour', cm.sent_at)::timestamptz AS bucket,
  COUNT(DISTINCT cm.id) AS message_count,
  MAX(cm.sent_at) AS last_sent
FROM channel_messages cm
JOIN packet_observations po ON po.packet_hash = cm.packet_hash
WHERE cm.sender_name IS NOT NULL
  AND cm.sent_at > NOW() - INTERVAL '30 days'
GROUP BY po.iata, cm.sender_name, date_trunc('hour', cm.sent_at);

CREATE UNIQUE INDEX idx_mv_top_talkers
  ON mv_top_talkers_by_iata(iata, sender_name, bucket);
