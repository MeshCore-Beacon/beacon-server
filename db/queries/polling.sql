-- name: UpsertPollingClaim :execrows
-- Only a newer claim replaces the stored one; zero rows means it was ignored.
INSERT INTO polling_claims (observer_public_key, observer_id, origin, client, policy_version, community, accepts_requests, request_slots, claimed_at, received_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (observer_public_key) DO UPDATE SET
    observer_id = EXCLUDED.observer_id, origin = EXCLUDED.origin, client = EXCLUDED.client,
    policy_version = EXCLUDED.policy_version, community = EXCLUDED.community,
    accepts_requests = EXCLUDED.accepts_requests, request_slots = EXCLUDED.request_slots,
    claimed_at = EXCLUDED.claimed_at, received_at = EXCLUDED.received_at
WHERE EXCLUDED.claimed_at > polling_claims.claimed_at;

-- name: DeletePollingCandidates :exec
DELETE FROM polling_candidates WHERE observer_public_key = $1;

-- name: InsertPollingCandidate :exec
INSERT INTO polling_candidates (observer_public_key, target_public_key, key_mode, claimed_snr, requested, state, misses, last_attempt, last_ok)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (observer_public_key, target_public_key) DO NOTHING;

-- name: InsertRepeaterReading :execrows
INSERT INTO repeater_readings (
    target_public_key, node_id, observer_id, sampled_at, received_at, method, tag,
    battery_mv, temperature_c, poll_snr,
    uptime_secs, packets_recv, packets_sent, recv_flood, recv_direct, sent_flood, sent_direct,
    tx_air_secs, rx_air_secs, recv_errors, noise_floor, last_rssi, tx_queue, flood_dups, direct_dups, err_events, last_snr,
    lpp, packet_hash, client)
VALUES (
    sqlc.arg(target_public_key), (SELECT n.id FROM nodes n WHERE n.public_key = sqlc.arg(target_public_key)),
    sqlc.arg(observer_id), sqlc.arg(sampled_at), sqlc.arg(received_at), sqlc.arg(method), sqlc.arg(tag),
    sqlc.narg(battery_mv), sqlc.narg(temperature_c), sqlc.narg(poll_snr),
    sqlc.narg(uptime_secs), sqlc.narg(packets_recv), sqlc.narg(packets_sent), sqlc.narg(recv_flood), sqlc.narg(recv_direct),
    sqlc.narg(sent_flood), sqlc.narg(sent_direct), sqlc.narg(tx_air_secs), sqlc.narg(rx_air_secs), sqlc.narg(recv_errors),
    sqlc.narg(noise_floor), sqlc.narg(last_rssi), sqlc.narg(tx_queue), sqlc.narg(flood_dups), sqlc.narg(direct_dups),
    sqlc.narg(err_events), sqlc.narg(last_snr),
    sqlc.narg(lpp), sqlc.narg(packet_hash), sqlc.arg(client))
ON CONFLICT (target_public_key, observer_id, tag, method) DO NOTHING;

-- name: HasLivePollingCandidate :one
SELECT EXISTS (
    SELECT 1 FROM polling_candidates c
    JOIN polling_claims cl ON cl.observer_public_key = c.observer_public_key
    WHERE c.observer_public_key = $1 AND c.target_public_key = $2 AND cl.received_at > $3
);

-- name: ListPollingTargets :many
-- A target with only expired claims is still listed while it has assignment rows, so the caller can clear them.
SELECT target_public_key::bytea AS target_public_key FROM (
    SELECT c.target_public_key FROM polling_candidates c
    JOIN polling_claims cl ON cl.observer_public_key = c.observer_public_key
    WHERE cl.received_at > $1
    UNION
    SELECT a.target_public_key FROM polling_assignments a
) t
ORDER BY target_public_key;

-- name: ListPollingCandidates :many
-- Evidence: zero-hop adverts of the target this observer heard since $3, with missing signal (0/0) left out. mean_snr is 0 when n is 0.
SELECT c.observer_public_key, c.claimed_snr, c.state, c.misses,
       COALESCE(po.blocked, false) AS blocked,
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

-- name: CurrentPollingPoller :one
SELECT observer_public_key FROM polling_assignments WHERE target_public_key = $1 AND rank = 0;

-- name: DeletePollingAssignments :exec
DELETE FROM polling_assignments WHERE target_public_key = $1;

-- name: InsertPollingAssignment :exec
INSERT INTO polling_assignments (target_public_key, observer_public_key, rank, evidence_snr, evidence_count, assigned_at)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: PollingOwnerRows :many
SELECT c.target_public_key, COALESCE(a.rank, -1)::int AS rank,
       (SELECT r.sampled_at FROM repeater_readings r WHERE r.target_public_key = c.target_public_key
        ORDER BY r.sampled_at DESC LIMIT 1)::timestamptz AS latest_ts
FROM polling_candidates c
JOIN polling_claims cl ON cl.observer_public_key = c.observer_public_key
LEFT JOIN polling_assignments a ON a.target_public_key = c.target_public_key AND a.observer_public_key = c.observer_public_key
WHERE c.observer_public_key = $1 AND cl.received_at > $2
ORDER BY c.target_public_key;

-- name: PollingObserverFlags :one
SELECT blocked, approved FROM polling_observers WHERE observer_public_key = $1;

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
WITH todo AS (
    SELECT x.id, x.packet_hash, x.observer_id, x.sampled_at FROM repeater_readings x
    WHERE x.packet_hash IS NOT NULL AND x.received_at >= $1
    ORDER BY x.id LIMIT $2
)
UPDATE repeater_readings r SET corroborated = (
    SELECT count(DISTINCT po.observer_id) FROM packet_observations po
    JOIN packets p ON p.packet_hash = po.packet_hash
    WHERE po.packet_hash = t.packet_hash AND po.observer_id <> t.observer_id
      AND p.payload_type IN (1, 8)
      AND po.heard_at BETWEEN t.sampled_at - interval '10 minutes' AND t.sampled_at + interval '10 minutes'
)::smallint
FROM todo t WHERE r.id = t.id;

-- name: DeleteExpiredPollingClaims :exec
DELETE FROM polling_claims WHERE received_at < $1;

-- name: DeleteOrphanPollingAssignments :exec
DELETE FROM polling_assignments a
WHERE NOT EXISTS (
    SELECT 1 FROM polling_candidates c
    WHERE c.target_public_key = a.target_public_key AND c.observer_public_key = a.observer_public_key
);

-- name: DeleteExpiredRepeaterReadings :exec
DELETE FROM repeater_readings WHERE received_at < $1;
