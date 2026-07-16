-- 008_packets_last_heard_index.sql
--
-- The packet list orders by last_heard_at but there was no index on it, so
-- every page seq-scanned and sorted all packets (~2.8s per page on a 1.5M
-- row table; 31ms with the index). Affordable to maintain now that presence
-- coalescing keeps this column from being rewritten on every observation.

CREATE INDEX idx_packets_last_heard ON packets(last_heard_at DESC);
