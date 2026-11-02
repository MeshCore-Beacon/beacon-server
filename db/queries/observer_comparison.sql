-- name: GetObserverComparison :one
-- Count packet identities, not reception rows: a broker repeat or another
-- reception by the same observer must not inflate the comparison.
--
-- Each observer's receptions are read on their own from idx_observations_observer
-- (observer_id, heard_at): (packet_hash, observer_id) is unique, so each side
-- holds a packet at most once, and the full join yields each heard packet once.
-- The route type is checked once per packet after the join.
-- The observer ids are scalar subqueries so the planner costs every observer
-- alike: given a busy observer's own row count, a custom plan may instead walk
-- every reception in the window through idx_observations_heard, which reads
-- several times the buffers.
WITH a AS (
    SELECT po.packet_hash
    FROM packet_observations po
    WHERE po.observer_id = (SELECT @observer_a::uuid)
      AND po.heard_at >= @since::timestamptz
      AND po.heard_at < @until::timestamptz
      AND (COALESCE(cardinality(@iatas::text[]), 0) = 0 OR po.iata = ANY(@iatas::text[]))
), b AS (
    SELECT po.packet_hash
    FROM packet_observations po
    WHERE po.observer_id = (SELECT @observer_b::uuid)
      AND po.heard_at >= @since::timestamptz
      AND po.heard_at < @until::timestamptz
      AND (COALESCE(cardinality(@iatas::text[]), 0) = 0 OR po.iata = ANY(@iatas::text[]))
), heard AS (
    SELECT COALESCE(a.packet_hash, b.packet_hash) AS packet_hash,
           a.packet_hash IS NOT NULL AS seen_a,
           b.packet_hash IS NOT NULL AS seen_b
    FROM a FULL JOIN b ON b.packet_hash = a.packet_hash
)
SELECT count(*) AS total_packets,
       count(*) FILTER (WHERE seen_a AND NOT seen_b) AS only_a,
       count(*) FILTER (WHERE seen_b AND NOT seen_a) AS only_b,
       count(*) FILTER (WHERE seen_a AND seen_b) AS both,
       EXISTS (SELECT 1 FROM observers WHERE id = @observer_a::uuid) AS known_a,
       EXISTS (SELECT 1 FROM observers WHERE id = @observer_b::uuid) AS known_b
FROM heard
JOIN packets p ON p.packet_hash = heard.packet_hash
WHERE p.route_type IN (0, 1);
