-- name: ExpireAlertOutbox :execrows
-- Drops what was not delivered within its window, and what was delivered long ago.
DELETE FROM alert_outbox
WHERE (delivered_at IS NULL AND created_at < sqlc.arg(undelivered_before))
   OR (delivered_at IS NOT NULL AND delivered_at < sqlc.arg(delivered_before));

-- name: PendingAlerts :many
SELECT o.id, o.user_id, o.pass_id, o.target_kind, o.target_public_key, o.name, o.alert_kind, COALESCE(o.value, 0)::bigint AS value,
       o.detail, o.attempts, u.email, (u.email_verified_at IS NOT NULL)::boolean AS email_verified,
       COALESCE(CASE WHEN o.target_kind = 'observer' THEN ob.id ELSE n.id END, '00000000-0000-0000-0000-000000000000'::uuid)::uuid AS target_id
FROM alert_outbox o
JOIN users u ON u.id = o.user_id AND u.disabled_at IS NULL
LEFT JOIN nodes n ON o.target_kind = 'node' AND n.public_key = o.target_public_key
LEFT JOIN observers ob ON o.target_kind = 'observer' AND ob.public_key = o.target_public_key
WHERE o.delivered_at IS NULL AND o.created_at >= sqlc.arg(not_before)
ORDER BY o.user_id, o.pass_id, o.id
LIMIT sqlc.arg(row_limit);

-- name: LiveUserDevices :many
SELECT d.user_id, d.platform, d.push_token
FROM user_devices d JOIN user_sessions s ON s.id = d.session_id
WHERE d.user_id = ANY(sqlc.arg(user_ids)::uuid[])
  AND s.ended_at IS NULL AND s.expires_at > now() AND (s.hard_expires_at IS NULL OR s.hard_expires_at > now());

-- name: MarkAlertsDelivered :exec
UPDATE alert_outbox SET delivered_at = sqlc.arg(at) WHERE id = ANY(sqlc.arg(ids)::bigint[]);

-- name: BumpAlertAttempts :exec
UPDATE alert_outbox SET attempts = attempts + 1 WHERE id = ANY(sqlc.arg(ids)::bigint[]);

-- name: DeleteExhaustedAlerts :execrows
DELETE FROM alert_outbox WHERE delivered_at IS NULL AND attempts >= sqlc.arg(max_attempts);

-- name: DeleteDevicesByToken :exec
DELETE FROM user_devices WHERE push_token = $1;
