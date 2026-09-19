-- name: GetObserverComparison :one
-- Count packet identities, not reception rows: a broker repeat or another
-- reception by the same observer must not inflate the comparison.
WITH heard AS (
    SELECT po.packet_hash,
           bool_or(po.observer_id = @observer_a::uuid) AS seen_a,
           bool_or(po.observer_id = @observer_b::uuid) AS seen_b
    FROM packet_observations po
    JOIN packets p ON p.packet_hash = po.packet_hash
    WHERE po.observer_id IN (@observer_a::uuid, @observer_b::uuid)
      AND po.heard_at >= @since::timestamptz
      AND po.heard_at < @until::timestamptz
      AND p.route_type IN (0, 1)
      AND (COALESCE(cardinality(@iatas::text[]), 0) = 0 OR po.iata = ANY(@iatas::text[]))
    GROUP BY po.packet_hash
)
SELECT count(*) AS total_packets,
       count(*) FILTER (WHERE seen_a AND NOT seen_b) AS only_a,
       count(*) FILTER (WHERE seen_b AND NOT seen_a) AS only_b,
       count(*) FILTER (WHERE seen_a AND seen_b) AS both,
       EXISTS (SELECT 1 FROM observers WHERE id = @observer_a::uuid) AS known_a,
       EXISTS (SELECT 1 FROM observers WHERE id = @observer_b::uuid) AS known_b
FROM heard;
