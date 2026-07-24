-- Copy payload_type onto the observation (like source_broker) so the breakdown
-- drops its join back to packets, which was overrunning /dev/shm and 500ing.

ALTER TABLE packet_observations ADD COLUMN payload_type SMALLINT;

-- Backfill the last 7 days only (all the breakdown reads); parallelism off so
-- the join spills to disk, not /dev/shm.
SET max_parallel_workers_per_gather = 0;

UPDATE packet_observations po
SET payload_type = p.payload_type
FROM packets p
WHERE p.packet_hash = po.packet_hash
  AND po.heard_at > NOW() - INTERVAL '7 days'
  AND po.payload_type IS NULL;

RESET max_parallel_workers_per_gather;
