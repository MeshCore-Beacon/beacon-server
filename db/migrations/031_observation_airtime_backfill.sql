-- Separate from 030 so the ALTER's ACCESS EXCLUSIVE lock is released before this backfill runs.

-- Backfill only: mirrors internal/lora.TimeOnAirMs (RadioLib getTimeOnAir, CRC on, explicit header).
CREATE FUNCTION lora_airtime_ms_backfill(frame_len int, sf int, bw real, cr int) RETURNS real
LANGUAGE sql IMMUTABLE AS $$
  SELECT (
    (CASE WHEN sf <= 8 THEN 32 ELSE 16 END) + 4.25
    + 8 + GREATEST(ceil((8*frame_len - 4*sf + 44)::double precision
                        / (4*(sf - 2*(CASE WHEN power(2, sf)/bw >= 16 THEN 1 ELSE 0 END)))) * cr, 0)
  ) * (power(2, sf)/bw)
$$;

-- Last 7 days like 015; parallelism off so the join spills to disk, not /dev/shm.
SET max_parallel_workers_per_gather = 0;

UPDATE packet_observations po
SET airtime_ms = lora_airtime_ms_backfill(
      1 + CASE WHEN p.transport_codes_present THEN 4 ELSE 0 END + 1
        + COALESCE(octet_length(po.path_bytes), 0) + octet_length(p.raw_payload),
      po.spread_factor, po.bandwidth_khz, po.coding_rate)
FROM packets p
WHERE p.packet_hash = po.packet_hash
  AND po.heard_at > NOW() - INTERVAL '7 days'
  AND po.airtime_ms IS NULL
  AND po.spread_factor BETWEEN 7 AND 12
  AND po.bandwidth_khz > 0
  AND po.coding_rate BETWEEN 5 AND 8;

RESET max_parallel_workers_per_gather;
DROP FUNCTION lora_airtime_ms_backfill(int, int, real, int);
