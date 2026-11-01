-- name: RecordScopeObservation :exec
WITH sample AS (
    INSERT INTO scope_seen_paths (hour, iata, scope_name, path_key)
    VALUES (date_trunc('hour', @heard_at::timestamptz, 'UTC'), @iata, @scope_name, @path_key)
    ON CONFLICT DO NOTHING RETURNING 1
), repeats AS (
    INSERT INTO node_scope_hourly (node_id, scope_name, hour, observations, first_seen, last_seen, min_hash_bytes, unique_samples, unique_first_seen)
    SELECT n.id, @scope_name, date_trunc('hour', @heard_at::timestamptz, 'UTC'), 1,
           @heard_at, @heard_at, @hash_bytes::smallint, (SELECT COUNT(*) FROM sample), @heard_at
    FROM nodes n WHERE n.id = ANY(@node_ids::uuid[]) AND n.node_type = 2 ORDER BY n.id
    ON CONFLICT (node_id, scope_name, hour) DO UPDATE SET
        observations = node_scope_hourly.observations + 1,
        first_seen = LEAST(node_scope_hourly.first_seen, EXCLUDED.first_seen),
        last_seen = GREATEST(node_scope_hourly.last_seen, EXCLUDED.last_seen),
        min_hash_bytes = LEAST(node_scope_hourly.min_hash_bytes, EXCLUDED.min_hash_bytes),
        unique_samples = node_scope_hourly.unique_samples + EXCLUDED.unique_samples,
        unique_first_seen = LEAST(node_scope_hourly.unique_first_seen, EXCLUDED.unique_first_seen)
)
INSERT INTO scope_receiver_hourly (hour, iata, scope_name, observer_id, reports, unique_samples, unresolved_hops, ambiguous_hops, first_seen, last_seen)
VALUES (date_trunc('hour', @heard_at::timestamptz, 'UTC'), @iata, @scope_name, @observer_id, 1,
        (SELECT COUNT(*) FROM sample), @unresolved_hops, @ambiguous_hops, @heard_at, @heard_at)
ON CONFLICT (hour, iata, scope_name, observer_id) DO UPDATE SET
    reports = scope_receiver_hourly.reports + 1,
    unique_samples = scope_receiver_hourly.unique_samples + EXCLUDED.unique_samples,
    unresolved_hops = scope_receiver_hourly.unresolved_hops + EXCLUDED.unresolved_hops,
    ambiguous_hops = scope_receiver_hourly.ambiguous_hops + EXCLUDED.ambiguous_hops,
    first_seen = LEAST(scope_receiver_hourly.first_seen, EXCLUDED.first_seen),
    last_seen = GREATEST(scope_receiver_hourly.last_seen, EXCLUDED.last_seen);

-- name: GetScopeReceiverActivity :many
SELECT s.*, COALESCE(o.display_name, encode(o.public_key, 'hex'))::text AS observer_name
FROM scope_receiver_hourly s JOIN observers o ON o.id=s.observer_id
WHERE s.hour >= @since::timestamptz AND s.hour < @until::timestamptz
  AND (cardinality(@iatas::text[]) = 0 OR s.iata = ANY(@iatas::text[]))
ORDER BY s.hour DESC,s.iata,s.scope_name,s.observer_id LIMIT 50001;

-- name: GetScopeRepeaterActivity :many
SELECT s.scope_name, COALESCE(s.hour,'epoch'::timestamptz)::timestamptz AS hour, GROUPING(s.hour)::integer AS total, COUNT(DISTINCT s.node_id)::bigint AS nodes,
       SUM(s.observations)::bigint AS reports
FROM node_scope_hourly s
WHERE s.hour >= @since::timestamptz AND s.hour < @until::timestamptz
  AND (cardinality(@iatas::text[]) = 0 OR EXISTS (
      SELECT 1 FROM node_iatas ni WHERE ni.node_id=s.node_id AND ni.iata::text=ANY(@iatas::text[]) AND ni.last_heard >= @since::timestamptz))
GROUP BY GROUPING SETS ((s.scope_name,s.hour),(s.scope_name)) ORDER BY s.hour,s.scope_name;

-- name: GetNodeScopeHistory :many
SELECT scope_name,hour,observations,unique_samples,first_seen,last_seen,unique_first_seen
FROM node_scope_hourly
WHERE node_id= @node_id AND hour>= @since::timestamptz AND hour< @until::timestamptz
ORDER BY hour,scope_name;

-- name: DeleteOldScopeSamples :execrows
WITH victims AS (SELECT hour,iata,scope_name,path_key FROM scope_seen_paths WHERE hour < @cutoff::timestamptz ORDER BY hour LIMIT 10000 FOR UPDATE SKIP LOCKED)
DELETE FROM scope_seen_paths s USING victims v WHERE (s.hour,s.iata,s.scope_name,s.path_key)=(v.hour,v.iata,v.scope_name,v.path_key);

-- name: DeleteOldScopeReceivers :execrows
WITH victims AS (SELECT hour,iata,scope_name,observer_id FROM scope_receiver_hourly WHERE hour < @cutoff::timestamptz ORDER BY hour LIMIT 10000 FOR UPDATE SKIP LOCKED)
DELETE FROM scope_receiver_hourly s USING victims v WHERE (s.hour,s.iata,s.scope_name,s.observer_id)=(v.hour,v.iata,v.scope_name,v.observer_id);
