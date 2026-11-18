-- name: ExpireAlertOutbox :execrows
-- Drops what was not delivered within its window, and what was delivered long ago.
DELETE FROM alert_outbox
WHERE (delivered_at IS NULL AND created_at < sqlc.arg(undelivered_before))
   OR (delivered_at IS NOT NULL AND delivered_at < sqlc.arg(delivered_before));

-- name: PendingAlerts :many
-- Notices of a finished pass (or one that started before pass_timeout_before and never finished) only, with a channel
-- that is not done and is due, and whole user and pass groups only, so one pass's notices for a user stay in one mail:
-- groups are taken in order while the running total stays within row_limit, and the first group is taken whatever
-- its size. email_due and push_due say which channels to try.
WITH p AS (
    SELECT o.id, o.user_id, o.pass_id, o.target_kind, o.target_public_key, o.name, o.alert_kind, o.value,
           o.detail, o.push_attempts, u.email, (u.email_verified_at IS NOT NULL)::boolean AS email_verified,
           u.alert_email, u.alert_push,
           (o.email_done_at IS NULL AND (o.email_next_attempt_at IS NULL OR o.email_next_attempt_at <= sqlc.arg(now)))::boolean AS email_due,
           (o.push_done_at IS NULL AND (o.push_next_attempt_at IS NULL OR o.push_next_attempt_at <= sqlc.arg(now)))::boolean AS push_due,
           COALESCE(CASE WHEN o.target_kind = 'observer' THEN ob.id ELSE n.id END, '00000000-0000-0000-0000-000000000000'::uuid)::uuid AS target_id
    FROM alert_outbox o
    JOIN users u ON u.id = o.user_id AND u.disabled_at IS NULL
    LEFT JOIN nodes n ON o.target_kind = 'node' AND n.public_key = o.target_public_key
    LEFT JOIN observers ob ON o.target_kind = 'observer' AND ob.public_key = o.target_public_key
    LEFT JOIN alert_passes ap ON ap.pass_id = o.pass_id
    WHERE (ap.pass_id IS NULL OR ap.finished_at IS NOT NULL OR ap.started_at < sqlc.arg(pass_timeout_before))
      AND o.delivered_at IS NULL AND o.created_at >= sqlc.arg(not_before)
      AND ((o.email_done_at IS NULL AND (o.email_next_attempt_at IS NULL OR o.email_next_attempt_at <= sqlc.arg(now)))
        OR (o.push_done_at IS NULL AND (o.push_next_attempt_at IS NULL OR o.push_next_attempt_at <= sqlc.arg(now))))
), g AS (
    SELECT user_id, pass_id, count(*) AS n, sum(count(*)) OVER (ORDER BY user_id, pass_id) AS running
    FROM p GROUP BY user_id, pass_id
)
SELECT p.id, p.user_id, p.pass_id, p.target_kind, p.target_public_key, p.name, p.alert_kind, p.value,
       p.detail, p.push_attempts, p.email, p.email_verified, p.alert_email, p.alert_push, p.email_due, p.push_due, p.target_id
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
SELECT d.session_id, d.user_id, d.platform, d.push_token, d.payload_key
FROM user_devices d JOIN user_sessions s ON s.id = d.session_id
WHERE d.user_id = ANY(sqlc.arg(user_ids)::uuid[])
  AND s.ended_at IS NULL AND s.expires_at > now() AND (s.hard_expires_at IS NULL OR s.hard_expires_at > now());

-- name: MarkAlertsEmailDone :exec
-- The mail went out (sent_at set) or none was needed.
UPDATE alert_outbox SET email_done_at = COALESCE(email_done_at, sqlc.arg(at)),
    email_sent_at = CASE WHEN sqlc.arg(sent)::boolean THEN COALESCE(email_sent_at, sqlc.arg(at)) ELSE email_sent_at END
WHERE id = ANY(sqlc.arg(ids)::bigint[]);

-- name: MarkAlertsPushDone :exec
UPDATE alert_outbox SET push_done_at = COALESCE(push_done_at, sqlc.arg(at)) WHERE id = ANY(sqlc.arg(ids)::bigint[]);

-- name: BumpAlertEmailAttempts :exec
-- Counts a failed mail and holds it back for a minute, doubling with each earlier failure; the last allowed failure
-- ends the channel.
UPDATE alert_outbox SET email_attempts = email_attempts + 1,
    email_next_attempt_at = GREATEST(sqlc.arg(at)::timestamptz + interval '1 minute' * power(2, email_attempts), sqlc.arg(hold_until)::timestamptz),
    email_done_at = CASE WHEN email_attempts + 1 >= sqlc.arg(max_attempts)::int THEN sqlc.arg(at) ELSE email_done_at END
WHERE id = ANY(sqlc.arg(ids)::bigint[]) AND email_done_at IS NULL;

-- name: BumpAlertPushAttempts :exec
-- As for mail, or held until hold_until when the push server asked for longer.
UPDATE alert_outbox SET push_attempts = push_attempts + 1,
    push_next_attempt_at = GREATEST(sqlc.arg(at)::timestamptz + interval '1 minute' * power(2, push_attempts), sqlc.arg(hold_until)::timestamptz),
    push_done_at = CASE WHEN push_attempts + 1 >= sqlc.arg(max_attempts)::int THEN sqlc.arg(at) ELSE push_done_at END
WHERE id = ANY(sqlc.arg(ids)::bigint[]) AND push_done_at IS NULL;

-- name: HoldAlertPush :exec
-- A rate-limited push waits until hold_until; no attempt is counted.
UPDATE alert_outbox SET push_next_attempt_at = sqlc.arg(hold_until)::timestamptz
WHERE id = ANY(sqlc.arg(ids)::bigint[]) AND push_done_at IS NULL;

-- name: DeleteExhaustedAlerts :execrows
-- A finished notice that ran out of attempts on a channel is dropped.
DELETE FROM alert_outbox
WHERE id = ANY(sqlc.arg(ids)::bigint[]) AND delivered_at IS NULL AND email_done_at IS NOT NULL AND push_done_at IS NOT NULL
  AND (email_attempts >= sqlc.arg(max_attempts)::int OR push_attempts >= sqlc.arg(max_attempts)::int);

-- name: MarkAlertsDelivered :exec
-- A notice is delivered once both channels are done.
UPDATE alert_outbox SET delivered_at = sqlc.arg(at)
WHERE id = ANY(sqlc.arg(ids)::bigint[]) AND delivered_at IS NULL AND email_done_at IS NOT NULL AND push_done_at IS NOT NULL;

-- name: DeleteDevicesByToken :exec
DELETE FROM user_devices WHERE push_token = $1;

-- name: DeliveredAlertDevices :many
SELECT outbox_id, session_id FROM alert_outbox_devices WHERE outbox_id = ANY(sqlc.arg(ids)::bigint[]);

-- name: MarkAlertDeviceDelivered :exec
INSERT INTO alert_outbox_devices (outbox_id, session_id, delivered_at)
VALUES ($1, $2, $3)
ON CONFLICT (outbox_id, session_id) DO NOTHING;
