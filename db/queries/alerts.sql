-- name: ListUserAlertRules :many
SELECT id, target_kind, target_public_key, kind, threshold, threshold_high, enabled, state, state_changed_at, last_sent_at
FROM user_alert_rules WHERE user_id = $1 ORDER BY created_at, id;

-- name: UserAlertRuleKinds :many
-- Only enabled rules: back_online needs a gone_quiet rule that is on.
SELECT kind FROM user_alert_rules WHERE user_id = $1 AND target_public_key = $2 AND enabled;

-- name: UserHasAtlasPin :one
SELECT EXISTS (SELECT 1 FROM user_atlas_pins WHERE user_id = $1 AND public_key = $2);

-- name: UpsertAlertRule :one
-- A change of thresholds, or turning a rule on, starts its state again: no stale notification, and the data the
-- evaluator looks at starts now.
INSERT INTO user_alert_rules (user_id, target_kind, target_public_key, kind, threshold, threshold_high, enabled)
VALUES (sqlc.arg(user_id), sqlc.arg(target_kind), sqlc.arg(target_public_key), sqlc.arg(kind), sqlc.narg(threshold),
        sqlc.narg(threshold_high), sqlc.arg(enabled))
ON CONFLICT (user_id, target_public_key, kind) DO UPDATE SET
    target_kind = EXCLUDED.target_kind,
    threshold = EXCLUDED.threshold,
    threshold_high = EXCLUDED.threshold_high,
    enabled = EXCLUDED.enabled,
    version = user_alert_rules.version + 1,
    state = CASE WHEN user_alert_rules.threshold IS DISTINCT FROM EXCLUDED.threshold
                   OR user_alert_rules.threshold_high IS DISTINCT FROM EXCLUDED.threshold_high
                   OR (EXCLUDED.enabled AND NOT user_alert_rules.enabled) THEN 'unknown' ELSE user_alert_rules.state END,
    state_changed_at = CASE WHEN user_alert_rules.threshold IS DISTINCT FROM EXCLUDED.threshold
                   OR user_alert_rules.threshold_high IS DISTINCT FROM EXCLUDED.threshold_high
                   OR (EXCLUDED.enabled AND NOT user_alert_rules.enabled) THEN now() ELSE user_alert_rules.state_changed_at END,
    cursor_at = CASE WHEN user_alert_rules.threshold IS DISTINCT FROM EXCLUDED.threshold
                   OR user_alert_rules.threshold_high IS DISTINCT FROM EXCLUDED.threshold_high
                   OR (EXCLUDED.enabled AND NOT user_alert_rules.enabled) THEN now() ELSE user_alert_rules.cursor_at END,
    streak = CASE WHEN user_alert_rules.threshold IS DISTINCT FROM EXCLUDED.threshold
                   OR user_alert_rules.threshold_high IS DISTINCT FROM EXCLUDED.threshold_high
                   OR (EXCLUDED.enabled AND NOT user_alert_rules.enabled) THEN 0 ELSE user_alert_rules.streak END
RETURNING id, target_kind, target_public_key, kind, threshold, threshold_high, enabled, state, state_changed_at, last_sent_at;

-- name: DeleteAlertRule :execrows
DELETE FROM user_alert_rules WHERE id = $1 AND user_id = $2;

-- name: DeleteUnpinnedAlertRules :exec
-- Run in the Atlas write's transaction: a radio taken off the Atlas takes its rules with it.
DELETE FROM user_alert_rules r
WHERE r.user_id = $1
  AND NOT EXISTS (SELECT 1 FROM user_atlas_pins p WHERE p.user_id = r.user_id AND p.public_key = r.target_public_key);

-- name: DeleteSessionDeviceOtherToken :exec
-- A session has one device: registering a new token replaces the old one.
DELETE FROM user_devices WHERE session_id = $1 AND push_token <> $2;

-- name: UpsertUserDevice :exec
-- The handle is unique, so a registration for a handle another session holds moves that row (crossing users on
-- purpose: a phone belongs to one account, so the previous user's alerts never reach its next user).
INSERT INTO user_devices (session_id, user_id, platform, push_token, payload_key)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (push_token) DO UPDATE
    SET session_id = EXCLUDED.session_id, user_id = EXCLUDED.user_id, platform = EXCLUDED.platform,
        payload_key = EXCLUDED.payload_key, updated_at = now();

-- name: TurnOnAlertPush :exec
UPDATE users SET alert_push = true WHERE id = $1;

-- name: UserAlertChannels :one
SELECT alert_email, alert_push, (email IS NOT NULL AND email <> '')::boolean AS has_email,
       (email_verified_at IS NOT NULL)::boolean AS email_verified
FROM users WHERE id = $1;

-- name: SetUserAlertChannels :execrows
UPDATE users SET alert_email = COALESCE(sqlc.narg(alert_email), alert_email), alert_push = COALESCE(sqlc.narg(alert_push), alert_push)
WHERE id = sqlc.arg(id);

-- name: DeleteUserDevice :exec
DELETE FROM user_devices WHERE session_id = $1 AND user_id = $2;

-- name: UserHasLiveDevice :one
SELECT EXISTS (
    SELECT 1 FROM user_devices d JOIN user_sessions s ON s.id = d.session_id
    WHERE d.user_id = $1 AND s.ended_at IS NULL AND s.expires_at > now() AND (s.hard_expires_at IS NULL OR s.hard_expires_at > now())
);

-- name: AlertNodeCaps :many
-- What each of the nodes is and what a polled one reports, over the readings since the cutoff.
SELECT n.id, n.public_key, n.node_type, COALESCE(n.name, '')::text AS name,
       COALESCE(c.readings, 0)::int AS readings,
       COALESCE(c.battery, false)::boolean AS has_battery, COALESCE(c.temperature, false)::boolean AS has_temperature,
       COALESCE(c.noise, false)::boolean AS has_noise, COALESCE(c.uptime, false)::boolean AS has_uptime,
       -- Polled is a current source, not history: some observer holds the repeater as a polling assignment, and polling is on.
       (sqlc.arg(polling_enabled)::boolean AND EXISTS (SELECT 1 FROM polling_assignments a WHERE a.target_public_key = n.public_key))::boolean AS polled
FROM nodes n
LEFT JOIN LATERAL (
    SELECT count(*) AS readings, bool_or(r.battery_mv IS NOT NULL) AS battery, bool_or(r.temperature_c IS NOT NULL) AS temperature,
           bool_or(r.noise_floor IS NOT NULL) AS noise, bool_or(r.uptime_secs IS NOT NULL) AS uptime
    FROM repeater_readings r
    WHERE r.target_public_key = n.public_key AND r.trusted AND r.sampled_at >= sqlc.arg(since)
) c ON true
WHERE n.public_key = ANY(sqlc.arg(keys)::bytea[]);

-- name: AlertObserverCaps :many
SELECT o.id, o.public_key, COALESCE(o.display_name, '')::text AS name,
       COALESCE(c.battery, false)::boolean AS has_battery, COALESCE(c.noise, false)::boolean AS has_noise,
       COALESCE(c.uptime, false)::boolean AS has_uptime,
       -- Polled is a current source, not history: status telemetry arrived since fresh_since.
       EXISTS (SELECT 1 FROM observer_telemetry f WHERE f.observer_id = o.id AND f.reported_at >= sqlc.arg(fresh_since)) AS polled
FROM observers o
LEFT JOIN LATERAL (
    SELECT bool_or(t.battery_voltage_mv IS NOT NULL) AS battery, bool_or(t.noise_floor_db IS NOT NULL) AS noise,
           bool_or(t.uptime_seconds IS NOT NULL) AS uptime
    FROM observer_telemetry t
    WHERE t.observer_id = o.id AND t.reported_at >= sqlc.arg(since)
) c ON true
WHERE o.public_key = ANY(sqlc.arg(keys)::bytea[]);
