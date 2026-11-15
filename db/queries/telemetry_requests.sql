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
