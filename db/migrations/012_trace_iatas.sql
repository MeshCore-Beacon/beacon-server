-- Per-IATA trace activity so the trace filter stops joining every trace packet
-- to observations (the hash join was overrunning /dev/shm).

CREATE TABLE trace_iatas (
  trace_tag  BYTEA NOT NULL,
  iata       CHAR(3) NOT NULL REFERENCES iata_codes(iata) ON DELETE CASCADE,
  last_heard TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (trace_tag, iata)
);

CREATE INDEX idx_trace_iatas_iata ON trace_iatas(iata);

-- Seed from retained packets; parallelism off so the join spills to disk, not /dev/shm.
SET max_parallel_workers_per_gather = 0;

INSERT INTO trace_iatas (trace_tag, iata, last_heard)
SELECT p.trace_tag, po.iata, MAX(po.heard_at)
FROM packets p
JOIN packet_observations po ON po.packet_hash = p.packet_hash
WHERE p.trace_tag IS NOT NULL
GROUP BY p.trace_tag, po.iata;

RESET max_parallel_workers_per_gather;
