-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: agpl

-- 003_tag_ping_traces.sql
UPDATE packets
SET parsed_payload = jsonb_set(parsed_payload, '{type}', '"PING"')
WHERE trace_tag IS NOT NULL
  AND jsonb_array_length(parsed_payload->'pathHashes') = 1
  AND parsed_payload->>'type' = 'TRACE';
