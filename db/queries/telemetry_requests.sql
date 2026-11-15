-- name: LockUserForRequests :one
-- Serializes one user's request creation (the limit) without blocking the foreign-key checks on their rows.
SELECT id FROM users WHERE id = $1 FOR NO KEY UPDATE;

-- name: CountUserRequests :one
SELECT count(*)::int FROM telemetry_requests WHERE user_id = $1;

-- name: EnsurePollingTarget :exec
INSERT INTO polling_targets (target_public_key) VALUES ($1) ON CONFLICT (target_public_key) DO NOTHING;

-- name: GetPollingTarget :one
SELECT active_request_id, ver, ver_changed_at, no_password_blocked FROM polling_targets WHERE target_public_key = $1;

-- name: UpdatePollingTarget :exec
UPDATE polling_targets SET active_request_id = $2, ver = $3, ver_changed_at = $4, no_password_blocked = $5
WHERE target_public_key = $1;

-- name: ListTargetRequests :many
SELECT id, user_id, status, status_reason, password_ct, password_nonce, password_failed, created_at, updated_at, enabled_at
FROM telemetry_requests WHERE target_public_key = $1 ORDER BY created_at, id;

-- name: InsertTelemetryRequest :exec
INSERT INTO telemetry_requests (id, user_id, target_public_key, status, status_reason, password_ct, password_nonce,
                                password_failed, created_at, updated_at, enabled_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);

-- name: UpdateTelemetryRequest :exec
UPDATE telemetry_requests SET status = $2, status_reason = $3, password_ct = $4, password_nonce = $5,
    password_failed = $6, updated_at = $7, enabled_at = $8
WHERE id = $1;

-- name: DeleteTelemetryRequest :exec
DELETE FROM telemetry_requests WHERE id = $1;

-- name: TelemetryRequestTarget :one
SELECT target_public_key FROM telemetry_requests WHERE id = $1 AND user_id = $2;

-- name: UserRequestTargets :many
SELECT DISTINCT target_public_key FROM telemetry_requests WHERE user_id = $1;

-- name: ListUserTelemetryRequests :many
-- A user's requests, oldest first, with the node's current id and name, the observer that polls the repeater and
-- the newest trusted reading. No password column is read; poller_id is the nil uuid when nobody polls it.
SELECT r.id, r.target_public_key, r.status, r.status_reason, (r.password_ct IS NOT NULL)::boolean AS has_password,
       r.created_at, r.updated_at,
       n.id AS node_id, COALESCE(n.name, '')::text AS node_name,
       COALESCE(p.observer_id, '00000000-0000-0000-0000-000000000000'::uuid) AS poller_id, COALESCE(p.name, '')::text AS poller_name,
       (SELECT max(x.sampled_at) FROM repeater_readings x WHERE x.target_public_key = r.target_public_key AND x.trusted)::timestamptz AS latest_reading_at
FROM telemetry_requests r
LEFT JOIN nodes n ON n.public_key = r.target_public_key
LEFT JOIN LATERAL (
    SELECT o.id AS observer_id, COALESCE(NULLIF(o.display_name, ''), left(encode(o.public_key, 'hex'), 8)) AS name
    FROM polling_assignments a JOIN observers o ON o.public_key = a.observer_public_key
    WHERE a.target_public_key = r.target_public_key AND a.rank = 0
    LIMIT 1
) p ON true
WHERE r.user_id = $1
ORDER BY r.created_at, r.id;

-- name: RequestNode :one
SELECT public_key, node_type, COALESCE(name, '')::text AS name FROM nodes WHERE id = $1;

-- name: InsertPollingAdminStop :execrows
INSERT INTO polling_admin_stops (target_public_key, observer_public_key, reported_at) VALUES ($1, $2, $3)
ON CONFLICT (target_public_key, observer_public_key) DO NOTHING;

-- name: DeletePollingAdminStop :execrows
DELETE FROM polling_admin_stops WHERE target_public_key = $1 AND observer_public_key = $2;

-- name: ListPollingAdminStops :many
SELECT target_public_key, observer_public_key, reported_at FROM polling_admin_stops ORDER BY reported_at DESC, target_public_key, observer_public_key;

-- name: ListNoPasswordBlocked :many
SELECT target_public_key FROM polling_targets WHERE no_password_blocked ORDER BY target_public_key;

-- name: ClearNoPasswordBlocked :execrows
-- Run under LockPollingTarget.
UPDATE polling_targets SET no_password_blocked = false WHERE target_public_key = $1 AND no_password_blocked;

-- name: PollingTargetExists :one
SELECT EXISTS (SELECT 1 FROM polling_targets WHERE target_public_key = $1);

-- name: TouchPollingOtherState :exec
INSERT INTO polling_other_states (target_public_key, observer_public_key, seen_at) VALUES ($1, $2, $3)
ON CONFLICT (target_public_key, observer_public_key) DO UPDATE SET seen_at = EXCLUDED.seen_at;

-- name: ListRequestedAssignees :many
-- The observers handed this target as a requested one, with their rank (0 polls).
SELECT observer_public_key, rank FROM polling_assignments WHERE target_public_key = $1 AND requested;

-- name: ListRequestedAssigneeReadings :many
-- When each observer handed this target as a requested one stored its newest trusted reading of it.
SELECT a.observer_public_key, l.received_at
FROM polling_assignments a
CROSS JOIN LATERAL (
    SELECT r.received_at FROM repeater_readings r
    WHERE r.target_public_key = a.target_public_key AND r.observer_public_key = a.observer_public_key AND r.trusted
    ORDER BY r.sampled_at DESC LIMIT 1
) l
WHERE a.target_public_key = $1 AND a.requested;

-- name: ListTargetAdminStops :many
SELECT observer_public_key FROM polling_admin_stops WHERE target_public_key = $1;

-- name: ListTargetServedVersions :many
SELECT observer_public_key, ver FROM polling_served_versions WHERE target_public_key = $1;

-- name: RecordServedVersion :exec
-- Written on every owners answer that lists the target, so it only touches the row when the version changed or the
-- last write is an hour old.
INSERT INTO polling_served_versions AS s (target_public_key, observer_public_key, ver, served_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (target_public_key, observer_public_key) DO UPDATE SET ver = EXCLUDED.ver, served_at = EXCLUDED.served_at
WHERE s.ver <> EXCLUDED.ver OR s.served_at < EXCLUDED.served_at - interval '1 hour';

-- name: DeleteOldServedVersions :exec
DELETE FROM polling_served_versions WHERE served_at < $1;

-- name: ListTargetOtherStates :many
SELECT observer_public_key, seen_at FROM polling_other_states WHERE target_public_key = $1;

-- name: OfflineRequestTargets :many
-- Targets with a waiting or active request whose node no observer has heard since cutoff and whose newest such
-- request was enabled before it. Requests with no node row count from the enabling alone.
SELECT r.target_public_key::bytea AS target_public_key
FROM telemetry_requests r
LEFT JOIN nodes n ON n.public_key = r.target_public_key
WHERE r.status IN ('waiting', 'active')
GROUP BY r.target_public_key, n.last_seen
HAVING GREATEST(max(r.enabled_at), COALESCE(n.last_seen, '-infinity'::timestamptz)) < sqlc.arg(cutoff)::timestamptz;

-- name: TargetStillOffline :one
-- OfflineRequestTargets for one target, asked again under the target's lock before its requests are disabled.
SELECT EXISTS (
    SELECT 1
    FROM telemetry_requests r
    LEFT JOIN nodes n ON n.public_key = r.target_public_key
    WHERE r.status IN ('waiting', 'active') AND r.target_public_key = sqlc.arg(target_public_key)
    GROUP BY r.target_public_key, n.last_seen
    HAVING GREATEST(max(r.enabled_at), COALESCE(n.last_seen, '-infinity'::timestamptz)) < sqlc.arg(cutoff)::timestamptz
)::boolean AS offline;

-- name: PollingAdminRequestCounts :many
SELECT r.target_public_key::bytea AS target_public_key, count(*)::int AS requests,
       bool_or(t.active_request_id IS NOT NULL)::boolean AS requested
FROM telemetry_requests r
LEFT JOIN polling_targets t ON t.target_public_key = r.target_public_key
WHERE r.target_public_key = ANY(sqlc.arg(targets)::bytea[])
GROUP BY r.target_public_key;
