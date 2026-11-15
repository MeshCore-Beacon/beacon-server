-- name: ExpireAlertOutbox :execrows
-- Drops what was not delivered within its window, and what was delivered long ago.
DELETE FROM alert_outbox
WHERE (delivered_at IS NULL AND created_at < sqlc.arg(undelivered_before))
   OR (delivered_at IS NOT NULL AND delivered_at < sqlc.arg(delivered_before));

-- name: PendingAlerts :many
-- Notices of a finished pass (or one that started before pass_timeout_before and never finished) only, and whole
-- user and pass groups only, so one pass's notices for a user stay in one mail: groups are taken in order
-- while the running total stays within row_limit, and the first group is taken whatever its size.
WITH p AS (
    SELECT o.id, o.user_id, o.pass_id, o.target_kind, o.target_public_key, o.name, o.alert_kind, COALESCE(o.value, 0)::bigint AS value,
           o.detail, o.attempts, u.email, (u.email_verified_at IS NOT NULL)::boolean AS email_verified,
           COALESCE(CASE WHEN o.target_kind = 'observer' THEN ob.id ELSE n.id END, '00000000-0000-0000-0000-000000000000'::uuid)::uuid AS target_id
    FROM alert_outbox o
    JOIN users u ON u.id = o.user_id AND u.disabled_at IS NULL
    LEFT JOIN nodes n ON o.target_kind = 'node' AND n.public_key = o.target_public_key
    LEFT JOIN observers ob ON o.target_kind = 'observer' AND ob.public_key = o.target_public_key
    LEFT JOIN alert_passes ap ON ap.pass_id = o.pass_id
    WHERE (ap.pass_id IS NULL OR ap.finished_at IS NOT NULL OR ap.started_at < sqlc.arg(pass_timeout_before))
      AND o.delivered_at IS NULL AND o.created_at >= sqlc.arg(not_before)
      AND (o.next_attempt_at IS NULL OR o.next_attempt_at <= sqlc.arg(now))
), g AS (
    SELECT user_id, pass_id, count(*) AS n, sum(count(*)) OVER (ORDER BY user_id, pass_id) AS running
    FROM p GROUP BY user_id, pass_id
)
SELECT p.id, p.user_id, p.pass_id, p.target_kind, p.target_public_key, p.name, p.alert_kind, p.value,
       p.detail, p.attempts, p.email, p.email_verified, p.target_id
FROM p JOIN g ON g.user_id = p.user_id AND g.pass_id = p.pass_id
WHERE g.running <= sqlc.arg(row_limit)::bigint OR g.running = g.n
ORDER BY p.user_id, p.pass_id, p.id;

-- name: BeginAlertPass :exec
INSERT INTO alert_passes (pass_id) VALUES ($1) ON CONFLICT DO NOTHING;

-- name: FinishAlertPass :exec
UPDATE alert_passes SET finished_at = now() WHERE pass_id = $1;

-- name: DeleteOldAlertPasses :exec
DELETE FROM alert_passes WHERE started_at < $1;

-- name: LiveUserDevices :many
SELECT d.session_id, d.user_id, d.platform, d.push_token
FROM user_devices d JOIN user_sessions s ON s.id = d.session_id
WHERE d.user_id = ANY(sqlc.arg(user_ids)::uuid[])
  AND s.ended_at IS NULL AND s.expires_at > now() AND (s.hard_expires_at IS NULL OR s.hard_expires_at > now());

-- name: MarkAlertsDelivered :exec
UPDATE alert_outbox SET delivered_at = sqlc.arg(at) WHERE id = ANY(sqlc.arg(ids)::bigint[]);

-- name: BumpAlertAttempts :exec
-- Counts a failed attempt and holds the notice back for a minute, doubling with each earlier failure.
UPDATE alert_outbox SET attempts = attempts + 1, next_attempt_at = sqlc.arg(at)::timestamptz + interval '1 minute' * power(2, attempts)
WHERE id = ANY(sqlc.arg(ids)::bigint[]);

-- name: DeleteExhaustedAlerts :execrows
DELETE FROM alert_outbox WHERE delivered_at IS NULL AND attempts >= sqlc.arg(max_attempts);

-- name: DeleteDevicesByToken :exec
DELETE FROM user_devices WHERE push_token = $1;

-- name: DeliveredAlertDevices :many
SELECT outbox_id, session_id FROM alert_outbox_devices WHERE outbox_id = ANY(sqlc.arg(ids)::bigint[]);

-- name: MarkAlertDeviceDelivered :exec
INSERT INTO alert_outbox_devices (outbox_id, session_id, delivered_at)
VALUES ($1, $2, $3)
ON CONFLICT (outbox_id, session_id) DO NOTHING;
