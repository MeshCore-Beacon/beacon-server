-- Per-IATA channel activity so the IATA filter skips the ~7s EXISTS over packets.
-- Keyed by raw hash (channels can share one), so no FK to channels.

CREATE TABLE channel_iatas (
  channel_hash BYTEA NOT NULL,
  iata         CHAR(3) NOT NULL REFERENCES iata_codes(iata) ON DELETE CASCADE,
  last_heard   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (channel_hash, iata)
);

CREATE INDEX idx_channel_iatas_iata ON channel_iatas(iata);

-- Seed from retained packets; parallelism off so the join spills to disk, not /dev/shm.
SET max_parallel_workers_per_gather = 0;

INSERT INTO channel_iatas (channel_hash, iata, last_heard)
SELECT p.channel_hash, po.iata, MAX(po.heard_at)
FROM packets p
JOIN packet_observations po ON po.packet_hash = p.packet_hash
WHERE p.channel_hash IS NOT NULL
GROUP BY p.channel_hash, po.iata;

RESET max_parallel_workers_per_gather;
