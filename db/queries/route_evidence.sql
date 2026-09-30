-- name: GetRouteEvidenceRoute :one
SELECT * FROM known_routes
WHERE iata = @iata::bpchar AND path_key = @path_key::bytea;

-- name: ListRouteEvidence :many
-- Leading index equalities and the time/ID boundary bound both custom and generic plans.
-- Full-byte equality is required even when the compact digest matches. TRACE path bytes
-- carry readings; unclassified legacy observations cannot be safely called ordinary paths.
SELECT po.id, po.packet_hash, po.observer_id, o.display_name AS observer_name,
       po.heard_at, po.payload_type, po.rssi, po.snr
FROM packet_observations po
JOIN observers o ON o.id = po.observer_id
WHERE po.iata = @iata::bpchar
  AND po.hash_size = @hash_size::smallint
  AND decode(md5(po.path_bytes), 'hex') = @path_digest::bytea
  AND po.path_bytes = @path_bytes::bytea
  AND po.hop_count >= 2
  AND po.hop_count = @hop_count::smallint
  AND po.path_bytes IS NOT NULL AND po.payload_type IS NOT NULL AND po.payload_type <> 9
  AND po.heard_at >= @since::timestamptz AND po.heard_at < @until::timestamptz
  AND (po.heard_at, po.id) < (@before_at::timestamptz, @before_id::bigint)
ORDER BY po.heard_at DESC, po.id DESC
LIMIT @page_limit::integer;
