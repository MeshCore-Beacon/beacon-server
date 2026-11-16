-- name: UpsertPollingClaim :execrows
-- Applies only a claim newer than the stored one and received at least the claim interval after it, checked in
-- this statement so concurrent claims cannot both pass. Zero rows means it was ignored.
INSERT INTO polling_claims (observer_public_key, observer_id, origin, client, policy_version, community, accepts_requests, request_slots, claimed_at, received_at)
VALUES (sqlc.arg(observer_public_key), sqlc.arg(observer_id), sqlc.arg(origin), sqlc.arg(client), sqlc.arg(policy_version),
        sqlc.arg(community), sqlc.arg(accepts_requests), sqlc.arg(request_slots), sqlc.arg(claimed_at), sqlc.arg(received_at))
ON CONFLICT (observer_public_key) DO UPDATE SET
    observer_id = EXCLUDED.observer_id, origin = EXCLUDED.origin, client = EXCLUDED.client,
    policy_version = EXCLUDED.policy_version, community = EXCLUDED.community,
    accepts_requests = EXCLUDED.accepts_requests, request_slots = EXCLUDED.request_slots,
    claimed_at = EXCLUDED.claimed_at, received_at = EXCLUDED.received_at
WHERE EXCLUDED.claimed_at > polling_claims.claimed_at AND polling_claims.received_at <= sqlc.arg(previous_received_by);

-- name: PollingClaimReceivedAt :one
SELECT received_at FROM polling_claims WHERE observer_public_key = $1;

-- name: DeletePollingCandidates :exec
DELETE FROM polling_candidates WHERE observer_public_key = $1;

-- name: InsertPollingCandidate :exec
INSERT INTO polling_candidates (observer_public_key, target_public_key, key_mode, claimed_snr, requested, state, misses, last_attempt, last_ok)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (observer_public_key, target_public_key) DO NOTHING;

-- name: InsertRepeaterReading :execrows
-- Run under LockPollingObserver. Inserts nothing when this observer stored a reading of the target by the same
-- method received after recent_after, or budget readings of any target received after budget_after.
INSERT INTO repeater_readings (
    target_public_key, observer_public_key, observer_id, sampled_at, received_at, method, tag,
    battery_mv, temperature_c, poll_snr,
    uptime_secs, packets_recv, packets_sent, recv_flood, recv_direct, sent_flood, sent_direct,
    tx_air_secs, rx_air_secs, recv_errors, noise_floor, last_rssi, tx_queue, flood_dups, direct_dups, err_events, last_snr,
    lpp, packet_hash, client, trusted)
SELECT
    sqlc.arg(target_public_key), sqlc.arg(observer_public_key),
    sqlc.arg(observer_id), sqlc.arg(sampled_at), sqlc.arg(received_at), sqlc.arg(method), sqlc.arg(tag),
    sqlc.narg(battery_mv), sqlc.narg(temperature_c), sqlc.narg(poll_snr),
    sqlc.narg(uptime_secs), sqlc.narg(packets_recv), sqlc.narg(packets_sent), sqlc.narg(recv_flood), sqlc.narg(recv_direct),
    sqlc.narg(sent_flood), sqlc.narg(sent_direct), sqlc.narg(tx_air_secs), sqlc.narg(rx_air_secs), sqlc.narg(recv_errors),
    sqlc.narg(noise_floor), sqlc.narg(last_rssi), sqlc.narg(tx_queue), sqlc.narg(flood_dups), sqlc.narg(direct_dups),
    sqlc.narg(err_events), sqlc.narg(last_snr),
    sqlc.narg(lpp), sqlc.narg(packet_hash), sqlc.arg(client),
    -- Same rule as latest_ts in PollingOwnerRows, frozen at insert time.
    EXISTS (
        SELECT 1 FROM polling_assignments t
        WHERE t.target_public_key = sqlc.arg(target_public_key) AND t.observer_public_key = sqlc.arg(observer_public_key)
          AND t.evidence_count > 0
    )
WHERE NOT EXISTS (
    SELECT 1 FROM repeater_readings x
    WHERE x.target_public_key = sqlc.arg(target_public_key) AND x.observer_public_key = sqlc.arg(observer_public_key)
      AND x.method = sqlc.arg(method) AND x.received_at > sqlc.arg(recent_after)
) AND (
    SELECT count(*) FROM repeater_readings b
    WHERE b.observer_public_key = sqlc.arg(observer_public_key) AND b.received_at > sqlc.arg(budget_after)
) < sqlc.arg(budget)::bigint
ON CONFLICT (target_public_key, observer_public_key, tag, method) DO NOTHING;

-- name: HasLivePollingCandidate :one
SELECT EXISTS (
    SELECT 1 FROM polling_candidates c
    JOIN polling_claims cl ON cl.observer_public_key = c.observer_public_key
    WHERE c.observer_public_key = $1 AND c.target_public_key = $2 AND cl.received_at > $3
);

-- name: ListPollingTargets :many
-- A target with only expired claims is still listed while it has assignment rows, so the caller can clear them. A
-- requested target is always listed, so its requests move between waiting and active.
SELECT target_public_key::bytea AS target_public_key FROM (
    SELECT c.target_public_key FROM polling_candidates c
    JOIN polling_claims cl ON cl.observer_public_key = c.observer_public_key
    WHERE cl.received_at > $1
    UNION
    SELECT a.target_public_key FROM polling_assignments a
    UNION
    SELECT pt.target_public_key FROM polling_targets pt WHERE pt.active_request_id IS NOT NULL
) t
ORDER BY target_public_key;

-- name: ListPollingCandidates :many
-- Evidence: zero-hop adverts of the target this observer heard since $3, with missing signal (0/0) left out. mean_snr is 0 when n is 0.
SELECT c.observer_public_key, c.claimed_snr, c.state, c.misses, c.requested,
       COALESCE(po.blocked, false) AS blocked, COALESCE(po.approved, false) AS approved,
       ev.n AS evidence_count, ev.mean_snr AS evidence_snr
FROM polling_candidates c
JOIN polling_claims cl ON cl.observer_public_key = c.observer_public_key
LEFT JOIN polling_observers po ON po.observer_public_key = c.observer_public_key
LEFT JOIN LATERAL (
    SELECT count(o.snr) AS n, COALESCE(avg(o.snr), 0)::float8 AS mean_snr
    FROM packets p
    JOIN packet_observations o ON o.packet_hash = p.packet_hash
    WHERE p.origin_pubkey = c.target_public_key AND p.payload_type = 4
      AND o.observer_id = cl.observer_id AND o.hop_count = 0
      AND p.first_heard_at >= $3 AND NOT (o.snr = 0 AND o.rssi = 0)
) ev ON true
WHERE c.target_public_key = $1 AND cl.received_at > $2
ORDER BY c.observer_public_key;

-- name: LockPollingTarget :exec
-- Serializes elections of one target until the transaction ends. Class 25190 in the two-key space, which never
-- meets the one-key migration lock; the schema keeps instances in other schemas apart.
SELECT pg_advisory_xact_lock(25190, hashtext(current_schema() || encode(sqlc.arg(target_public_key)::bytea, 'hex')));

-- name: LockPollingObserver :exec
-- Serializes one observer's reading inserts until the transaction ends, so its rate and budget checks see each
-- other. Class 25191, beside the target class and the (25189, n) job locks.
SELECT pg_advisory_xact_lock(25191, hashtext(current_schema() || encode(sqlc.arg(observer_public_key)::bytea, 'hex')));

-- name: ListRequestObserverKeys :many
-- The observers whose claim accepts requests, in key order, which is the order their seat locks are taken in.
SELECT observer_public_key FROM polling_claims WHERE accepts_requests AND received_at > $1 ORDER BY observer_public_key;

-- name: LockPollingSeats :exec
-- Serializes the seat decisions of one observer until the transaction ends. Class 25192, beside the target class
-- (25190) and the reading-insert class (25191). Taken before the target lock, in key order, so two elections
-- cannot wait on each other.
SELECT pg_advisory_xact_lock(25192, hashtext(current_schema() || encode(sqlc.arg(observer_public_key)::bytea, 'hex')));

-- name: ListObserverRequestedSeats :many
-- The observer's requested assignments to other targets that are still requested, with the age of each target's
-- oldest request, oldest first. A withdrawn target's assignment waits for its own election and holds no seat.
SELECT x.target_public_key::bytea AS target_public_key, q.oldest::timestamptz AS oldest
FROM polling_assignments x
JOIN polling_targets pt ON pt.target_public_key = x.target_public_key AND pt.active_request_id IS NOT NULL
JOIN LATERAL (SELECT min(r.created_at) AS oldest FROM telemetry_requests r WHERE r.target_public_key = x.target_public_key) q ON true
WHERE x.observer_public_key = sqlc.arg(observer_public_key) AND x.requested
  AND x.target_public_key <> sqlc.arg(target_public_key) AND q.oldest IS NOT NULL
ORDER BY q.oldest, x.target_public_key;

-- name: OldestTargetRequest :one
SELECT min(created_at)::timestamptz AS oldest FROM telemetry_requests WHERE target_public_key = $1;

-- name: DeleteObserverTargetAssignment :execrows
-- Run under LockPollingTarget, then CompactTargetAssignments.
DELETE FROM polling_assignments WHERE target_public_key = $1 AND observer_public_key = $2;

-- name: CompactTargetAssignments :exec
-- Renumbers the target's ranks 0..n-1 in their current order, so after a delete the best survivor polls. Only the
-- lowest rank can become 0, so the one-poller index holds row by row. Run under LockPollingTarget.
UPDATE polling_assignments a SET rank = r.new_rank
FROM (
    SELECT observer_public_key, (row_number() OVER (ORDER BY rank, observer_public_key) - 1)::smallint AS new_rank
    FROM polling_assignments WHERE target_public_key = sqlc.arg(target_public_key)
) r
WHERE a.target_public_key = sqlc.arg(target_public_key) AND a.observer_public_key = r.observer_public_key AND a.rank <> r.new_rank;

-- name: ListRequestedAssignedTargets :many
-- The targets that hold a requested assignment of any of the observers: the ones a seat decision may take a seat
-- from, locked with the election in key order.
SELECT DISTINCT target_public_key::bytea AS target_public_key FROM polling_assignments
WHERE requested AND observer_public_key = ANY(sqlc.arg(observers)::bytea[])
ORDER BY 1;

-- name: CountTargetAssignments :one
SELECT count(*)::int FROM polling_assignments WHERE target_public_key = $1;

-- name: CurrentPollingPoller :one
SELECT observer_public_key FROM polling_assignments WHERE target_public_key = $1 AND rank = 0;

-- name: DeletePollingAssignments :exec
DELETE FROM polling_assignments WHERE target_public_key = $1;

-- name: InsertPollingAssignment :exec
INSERT INTO polling_assignments (target_public_key, observer_public_key, rank, evidence_snr, evidence_count, assigned_at, requested)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: ListRequestCandidates :many
-- Observers that could take a requested target: a live claim that accepts requests and zero-hop advert evidence for
-- the target since first_heard_at (missing signal 0/0 left out). They need not name the target in their claim: a
-- device learns of a requested target from the owners answer. held counts the requested targets the claim names;
-- seats_elsewhere counts this observer's requested assignments to other still-requested targets whose oldest request
-- is older than this target's, so seats go to the oldest requests first.
WITH me AS (
    SELECT min(created_at) AS oldest FROM telemetry_requests WHERE target_public_key = sqlc.arg(target_public_key)
), ev AS (
    SELECT o.observer_id, count(o.snr) AS n, COALESCE(avg(o.snr), 0)::float8 AS mean_snr
    FROM packets p
    JOIN packet_observations o ON o.packet_hash = p.packet_hash
    WHERE p.origin_pubkey = sqlc.arg(target_public_key) AND p.payload_type = 4
      AND o.hop_count = 0 AND p.first_heard_at >= sqlc.arg(first_heard_at) AND NOT (o.snr = 0 AND o.rssi = 0)
    GROUP BY o.observer_id
)
SELECT cl.observer_public_key, c.claimed_snr, COALESCE(c.state, '')::text AS state, COALESCE(c.misses, 0)::smallint AS misses,
       COALESCE(c.requested, false)::boolean AS requested,
       COALESCE(po.blocked, false) AS blocked, COALESCE(po.approved, false) AS approved,
       cl.request_slots::int AS request_slots,
       (SELECT count(*) FROM polling_candidates h WHERE h.observer_public_key = cl.observer_public_key AND h.requested)::int AS held,
       EXISTS (SELECT 1 FROM polling_admin_stops s
               WHERE s.target_public_key = sqlc.arg(target_public_key) AND s.observer_public_key = cl.observer_public_key) AS admin_stopped,
       (SELECT count(*) FROM polling_assignments x
        JOIN polling_targets pt ON pt.target_public_key = x.target_public_key AND pt.active_request_id IS NOT NULL
        JOIN LATERAL (SELECT min(r.created_at) AS oldest FROM telemetry_requests r WHERE r.target_public_key = x.target_public_key) q ON true
        WHERE x.observer_public_key = cl.observer_public_key AND x.requested
          AND x.target_public_key <> sqlc.arg(target_public_key) AND q.oldest IS NOT NULL
          AND (q.oldest, x.target_public_key) < (me.oldest, sqlc.arg(target_public_key)::bytea))::int AS seats_elsewhere,
       ev.n AS evidence_count, ev.mean_snr AS evidence_snr
FROM polling_claims cl
CROSS JOIN me
JOIN ev ON ev.observer_id = cl.observer_id AND ev.n > 0
LEFT JOIN polling_candidates c ON c.observer_public_key = cl.observer_public_key AND c.target_public_key = sqlc.arg(target_public_key)
LEFT JOIN polling_observers po ON po.observer_public_key = cl.observer_public_key
WHERE cl.accepts_requests AND cl.received_at > sqlc.arg(received_after)
ORDER BY cl.observer_public_key;

-- name: PollingOwnerRows :many
-- assigned counts the target's assignment rows. latest_ts only trusts readings from an assigned observer with
-- advert evidence (the newest per assignee, then the newest of those), so an observer Beacon has never heard the repeater through cannot stand others down. A target the
-- observer was handed as a requested one is listed even before its claim names it (that is how it learns of it);
-- claimed_requested is true when the claim names it as requested; target_requested when a request is in use for it.
-- accepts_requests and request_slots are the claim's, for polling.OfferSeats. The observer's flags and the password
-- in use come from the same statement, so an answer never pairs a password with an approval it did not exist under.
-- The password is only read for a requested assignment of an unblocked observer whose claim accepts requests.
WITH mine AS (
    SELECT c.target_public_key FROM polling_candidates c WHERE c.observer_public_key = sqlc.arg(observer_public_key)
    UNION
    SELECT a.target_public_key FROM polling_assignments a
    WHERE a.observer_public_key = sqlc.arg(observer_public_key) AND a.requested
)
SELECT m.target_public_key::bytea AS target_public_key, COALESCE(a.rank, -1)::int AS rank,
       (SELECT count(*) FROM polling_assignments x WHERE x.target_public_key = m.target_public_key)::int AS assigned,
       (SELECT max(l.sampled_at) FROM polling_assignments t
        CROSS JOIN LATERAL (
            SELECT r.sampled_at FROM repeater_readings r
            WHERE r.target_public_key = t.target_public_key AND r.observer_public_key = t.observer_public_key
            ORDER BY r.sampled_at DESC LIMIT 1) l
        WHERE t.target_public_key = m.target_public_key AND t.evidence_count > 0)::timestamptz AS latest_ts,
       COALESCE(a.requested, false)::boolean AS requested,
       COALESCE(pc.requested, false)::boolean AS claimed_requested,
       EXISTS (SELECT 1 FROM polling_targets pt
               WHERE pt.target_public_key = m.target_public_key AND pt.active_request_id IS NOT NULL) AS target_requested,
       cl.accepts_requests, cl.request_slots::int AS request_slots,
       COALESCE(po.approved, false)::boolean AS observer_approved, COALESCE(po.blocked, false)::boolean AS observer_blocked,
       pr.id AS request_id, pt.ver AS password_ver, pr.password_ct, pr.password_nonce
FROM mine m
JOIN polling_claims cl ON cl.observer_public_key = sqlc.arg(observer_public_key)
LEFT JOIN polling_observers po ON po.observer_public_key = cl.observer_public_key
LEFT JOIN polling_candidates pc ON pc.observer_public_key = cl.observer_public_key AND pc.target_public_key = m.target_public_key
LEFT JOIN polling_assignments a ON a.target_public_key = m.target_public_key AND a.observer_public_key = cl.observer_public_key
LEFT JOIN polling_targets pt ON pt.target_public_key = m.target_public_key
    AND a.requested AND cl.accepts_requests AND NOT COALESCE(po.blocked, false)
LEFT JOIN telemetry_requests pr ON pr.id = pt.active_request_id
WHERE cl.received_at > sqlc.arg(received_at)
ORDER BY m.target_public_key;

-- name: PollingObserverFlags :one
SELECT blocked, approved FROM polling_observers WHERE observer_public_key = $1;

-- name: ListPollingObservers :many
SELECT observer_public_key, blocked, approved, note FROM polling_observers ORDER BY observer_public_key;

-- name: TryPollingElectionLock :one
SELECT pg_try_advisory_lock(25189, 2);

-- name: PollingElectionUnlock :exec
SELECT pg_advisory_unlock(25189, 2);

-- name: SetPollingObserver :exec
INSERT INTO polling_observers (observer_public_key, blocked, approved, note) VALUES ($1, $2, $3, $4)
ON CONFLICT (observer_public_key) DO UPDATE SET blocked = EXCLUDED.blocked, approved = EXCLUDED.approved, note = EXCLUDED.note;

-- name: GetPollingState :one
SELECT paused, paused_until, excluded_nodes FROM polling_state WHERE id;

-- name: SetPollingState :exec
UPDATE polling_state SET paused = $1, paused_until = $2, excluded_nodes = $3 WHERE id;

-- name: CorroborateReadings :execrows
-- Recomputed each pass. Counts other observers that heard a RESPONSE (1) or PATH (8) packet with the reading's
-- 8-byte hash within 10 minutes of sampled_at. packets and packet_observations keep no source hash, so the
-- sender is not checked; packet_observations.payload_type is not always filled, so the type comes from packets.
-- A reading whose observer row was deleted counts every observer. Never-counted readings go first, oldest first,
-- then counted ones newest first, so every reading is reached however many arrive.
WITH todo AS (
    SELECT x.id, x.packet_hash, x.observer_id, x.sampled_at FROM repeater_readings x
    WHERE x.packet_hash IS NOT NULL AND x.received_at >= $1
    ORDER BY x.corroborated IS NOT NULL, CASE WHEN x.corroborated IS NULL THEN x.id ELSE -x.id END LIMIT $2
)
UPDATE repeater_readings r SET corroborated = (
    SELECT count(DISTINCT po.observer_id) FROM packet_observations po
    JOIN packets p ON p.packet_hash = po.packet_hash
    WHERE po.packet_hash = t.packet_hash AND po.observer_id IS DISTINCT FROM t.observer_id
      AND p.payload_type IN (1, 8)
      AND po.heard_at BETWEEN t.sampled_at - interval '10 minutes' AND t.sampled_at + interval '10 minutes'
)::smallint
FROM todo t WHERE r.id = t.id;

-- name: DeleteExpiredPollingClaims :exec
DELETE FROM polling_claims WHERE received_at < $1;

-- name: ListOrphanPollingTargets :many
-- Targets with an assignment whose candidate is gone. A requested assignment is kept while its observer's claim
-- accepts requests: it is made before the device lists the target, and the claim rows of an expired claim are gone.
SELECT DISTINCT a.target_public_key FROM polling_assignments a
WHERE NOT EXISTS (
    SELECT 1 FROM polling_candidates c
    WHERE c.target_public_key = a.target_public_key AND c.observer_public_key = a.observer_public_key
) AND NOT (a.requested AND EXISTS (
    SELECT 1 FROM polling_claims cl WHERE cl.observer_public_key = a.observer_public_key AND cl.accepts_requests
))
ORDER BY a.target_public_key;

-- name: DeleteOrphanPollingAssignments :exec
-- Run under LockPollingTarget, so it cannot interleave with an election of the target.
DELETE FROM polling_assignments a
WHERE a.target_public_key = $1 AND NOT EXISTS (
    SELECT 1 FROM polling_candidates c
    WHERE c.target_public_key = a.target_public_key AND c.observer_public_key = a.observer_public_key
) AND NOT (a.requested AND EXISTS (
    SELECT 1 FROM polling_claims cl WHERE cl.observer_public_key = a.observer_public_key AND cl.accepts_requests
));

-- name: DeleteExpiredRepeaterReadings :exec
DELETE FROM repeater_readings WHERE received_at < $1;

-- name: PollingAdminTargets :many
-- Targets with a live candidate or any assignment row, newest claim first. latest_trusted uses the owners rule.
WITH t AS (
    SELECT c.target_public_key, max(cl.received_at) AS newest_claim
    FROM polling_candidates c
    JOIN polling_claims cl ON cl.observer_public_key = c.observer_public_key
    WHERE cl.received_at > sqlc.arg(received_after)
    GROUP BY c.target_public_key
    UNION ALL
    SELECT a.target_public_key, NULL::timestamptz FROM polling_assignments a GROUP BY a.target_public_key
    UNION ALL
    SELECT r.target_public_key, NULL::timestamptz FROM telemetry_requests r GROUP BY r.target_public_key
), picked AS (
    SELECT t.target_public_key, max(t.newest_claim) AS newest_claim FROM t
    GROUP BY t.target_public_key
    ORDER BY max(t.newest_claim) DESC NULLS LAST, t.target_public_key
    LIMIT sqlc.arg(row_limit)
)
SELECT p.target_public_key::bytea AS target_public_key,
       (SELECT max(l.sampled_at) FROM polling_assignments x
        CROSS JOIN LATERAL (
            SELECT r.sampled_at FROM repeater_readings r
            WHERE r.target_public_key = x.target_public_key AND r.observer_public_key = x.observer_public_key
            ORDER BY r.sampled_at DESC LIMIT 1) l
        WHERE x.target_public_key = p.target_public_key AND x.evidence_count > 0)::timestamptz AS latest_trusted,
       (SELECT r.sampled_at FROM repeater_readings r WHERE r.target_public_key = p.target_public_key
        ORDER BY r.sampled_at DESC LIMIT 1)::timestamptz AS latest_any
FROM picked p
ORDER BY p.newest_claim DESC NULLS LAST, p.target_public_key;

-- name: PollingAdminTargetRows :many
-- Each live candidate and each assigned observer of the given targets. rank is -1 for an unassigned candidate.
-- Claim fields come only from a live claim, so an assignment left over from an expired one shows them empty.
SELECT k.target_public_key::bytea AS target_public_key, k.observer_public_key::bytea AS observer_public_key,
       COALESCE(a.rank, -1)::int AS rank, a.evidence_snr, COALESCE(a.evidence_count, 0)::int AS evidence_count,
       COALESCE(c.key_mode, '')::text AS key_mode, COALESCE(c.state, '')::text AS state,
       COALESCE(c.misses, 0)::int AS misses, c.last_ok, COALESCE(po.approved, false) AS approved
FROM (
    SELECT c.target_public_key, c.observer_public_key FROM polling_candidates c
    JOIN polling_claims cl ON cl.observer_public_key = c.observer_public_key
    WHERE c.target_public_key = ANY(sqlc.arg(targets)::bytea[]) AND cl.received_at > sqlc.arg(received_after)
    UNION
    SELECT a.target_public_key, a.observer_public_key FROM polling_assignments a
    WHERE a.target_public_key = ANY(sqlc.arg(targets)::bytea[])
) k
LEFT JOIN (
    SELECT lc.target_public_key, lc.observer_public_key, lc.key_mode, lc.state, lc.misses, lc.last_ok
    FROM polling_candidates lc
    JOIN polling_claims lcl ON lcl.observer_public_key = lc.observer_public_key
    WHERE lc.target_public_key = ANY(sqlc.arg(targets)::bytea[]) AND lcl.received_at > sqlc.arg(received_after)
) c ON c.target_public_key = k.target_public_key AND c.observer_public_key = k.observer_public_key
LEFT JOIN polling_assignments a ON a.target_public_key = k.target_public_key AND a.observer_public_key = k.observer_public_key
LEFT JOIN polling_observers po ON po.observer_public_key = k.observer_public_key
ORDER BY k.target_public_key, COALESCE(a.rank, 32767), k.observer_public_key;

-- name: NodePublicKey :one
SELECT public_key FROM nodes WHERE id = $1;

-- name: ListNodeReadings :many
-- Readings of one target in [since, until) ascending; until is open when NULL. Untrusted ones only when asked for.
SELECT sampled_at, method, trusted, battery_mv, temperature_c, noise_floor, last_rssi, last_snr, uptime_secs, tx_queue,
       tx_air_secs, rx_air_secs, packets_recv, packets_sent, recv_errors
FROM repeater_readings
WHERE target_public_key = @target_public_key
  AND sampled_at >= @since::timestamptz
  AND (sqlc.narg(until)::timestamptz IS NULL OR sampled_at < sqlc.narg(until)::timestamptz)
  AND (trusted OR @include_untrusted::boolean)
ORDER BY sampled_at ASC, id ASC;

-- name: CountUntrustedNodeReadings :one
SELECT count(*)::int FROM repeater_readings
WHERE target_public_key = @target_public_key
  AND sampled_at >= @since::timestamptz
  AND (sqlc.narg(until)::timestamptz IS NULL OR sampled_at < sqlc.narg(until)::timestamptz)
  AND NOT trusted;

-- name: LatestNodeStatusReadingBefore :many
-- The newest status reading before since: the baseline for the first counter increments in range.
SELECT sampled_at, method, trusted, battery_mv, temperature_c, noise_floor, last_rssi, last_snr, uptime_secs, tx_queue,
       tx_air_secs, rx_air_secs, packets_recv, packets_sent, recv_errors
FROM repeater_readings
WHERE target_public_key = @target_public_key
  AND sampled_at < @since::timestamptz
  AND method = 'status'
  AND (trusted OR @include_untrusted::boolean)
ORDER BY sampled_at DESC, id DESC
LIMIT 1;

-- name: NodeHasReadings :one
SELECT EXISTS (SELECT 1 FROM repeater_readings WHERE target_public_key = $1);

-- name: NodePollingAssignments :many
-- Assigned observers of one target by rank. An assignment whose observer has no row is left out.
SELECT o.id AS observer_id, a.rank::int AS rank,
       COALESCE(NULLIF(o.display_name, ''), left(encode(o.public_key, 'hex'), 8))::text AS name
FROM polling_assignments a
JOIN observers o ON o.public_key = a.observer_public_key
WHERE a.target_public_key = $1
ORDER BY a.rank, o.id;

-- name: NodeLatestTrustedReadingAt :one
SELECT max(sampled_at)::timestamptz AS sampled_at FROM repeater_readings WHERE target_public_key = $1 AND trusted;

-- name: LatestNodeReadings :many
-- Newest trusted battery and temperature per polled node since a time, driven by polled targets, not all nodes.
-- The received_at bound lets the BRIN index prune the history scan for readings of unassigned targets.
WITH targets AS (
  SELECT target_public_key FROM polling_assignments
  UNION
  SELECT DISTINCT target_public_key FROM repeater_readings WHERE trusted AND sampled_at >= @since::timestamptz AND received_at >= @since::timestamptz
)
SELECT n.id AS node_id, b.battery_mv, t.temperature_c,
       GREATEST(b.sampled_at, t.sampled_at)::timestamptz AS sampled_at
FROM targets tg
JOIN nodes n ON n.public_key = tg.target_public_key
LEFT JOIN LATERAL (
  SELECT r.battery_mv, r.sampled_at FROM repeater_readings r
  WHERE r.target_public_key = n.public_key AND r.trusted AND r.battery_mv IS NOT NULL AND r.sampled_at >= @since::timestamptz
  ORDER BY r.sampled_at DESC, r.id DESC LIMIT 1
) b ON true
LEFT JOIN LATERAL (
  SELECT r.temperature_c, r.sampled_at FROM repeater_readings r
  WHERE r.target_public_key = n.public_key AND r.trusted AND r.temperature_c IS NOT NULL AND r.sampled_at >= @since::timestamptz
  ORDER BY r.sampled_at DESC, r.id DESC LIMIT 1
) t ON true
WHERE (b.sampled_at IS NOT NULL OR t.sampled_at IS NOT NULL)
  AND (COALESCE(cardinality(@iatas::bpchar[]), 0) = 0 OR n.id IN (SELECT node_id FROM node_iatas WHERE iata = ANY(@iatas::bpchar[])))
ORDER BY n.id;
