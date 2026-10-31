-- name: RecordScopeRepeats :exec
-- One accepted MQTT hearing, one vote per distinct resolved repeater. These are
-- repeat observations, NOT unique RF transmissions or configuration claims.
INSERT INTO node_scope_hourly (node_id, scope_name, hour, observations, first_seen, last_seen, min_hash_bytes)
SELECT n.id, @scope_name::text, date_trunc('hour', @heard_at::timestamptz), 1,
       @heard_at::timestamptz, @heard_at::timestamptz, @hash_bytes::smallint
FROM nodes n WHERE n.id = ANY(@node_ids::uuid[]) AND n.node_type = 2
ORDER BY n.id
ON CONFLICT (node_id, scope_name, hour) DO UPDATE SET
    observations = node_scope_hourly.observations + 1,
    first_seen = LEAST(node_scope_hourly.first_seen, EXCLUDED.first_seen),
    last_seen = GREATEST(node_scope_hourly.last_seen, EXCLUDED.last_seen),
    min_hash_bytes = LEAST(node_scope_hourly.min_hash_bytes, EXCLUDED.min_hash_bytes);

-- name: GetNodeScopeEvidence :many
-- Page IDs keep work bounded; no packet-history scan or external lookup.
SELECT s.node_id, 'repeat'::text AS source, s.scope_name AS scope,
       SUM(s.observations)::bigint AS observations, MAX(s.last_seen)::timestamptz AS checked_at,
       MIN(s.min_hash_bytes)::smallint AS hash_bytes
FROM node_scope_hourly s
WHERE s.node_id = ANY(@node_ids::uuid[])
  AND s.hour >= date_trunc('hour', @since::timestamptz)
  AND s.hour < date_trunc('hour', @until::timestamptz) + interval '1 hour'
GROUP BY s.node_id,s.scope_name
UNION ALL
SELECT nn.neighbor_id, 'reported', nn.region_scope, 0::bigint, nn.region_scope_checked_at, 0::smallint
FROM node_neighbors nn
WHERE nn.neighbor_id = ANY(@node_ids::uuid[]) AND nn.region_scope IS NOT NULL
UNION ALL
SELECT n.id, 'self', o.region_scope, 0::bigint, o.region_scope_checked_at, 0::smallint
FROM nodes n JOIN observers o ON o.public_key=n.public_key
WHERE n.id = ANY(@node_ids::uuid[]) AND o.region_scope IS NOT NULL
UNION ALL
SELECT n.id, 'heard', ts.name, 0::bigint, os.last_seen, 0::smallint
FROM nodes n JOIN observers o ON o.public_key=n.public_key
JOIN observer_scopes os ON os.observer_id=o.id JOIN transport_scopes ts ON ts.id=os.scope_id
WHERE n.id = ANY(@node_ids::uuid[]);

-- name: DeleteOldScopeRepeats :execrows
WITH victims AS (
    SELECT node_id,scope_name,hour FROM node_scope_hourly WHERE hour < @cutoff::timestamptz
    ORDER BY hour LIMIT 10000 FOR UPDATE SKIP LOCKED
)
DELETE FROM node_scope_hourly s USING victims v
WHERE s.node_id=v.node_id AND s.scope_name=v.scope_name AND s.hour=v.hour;
