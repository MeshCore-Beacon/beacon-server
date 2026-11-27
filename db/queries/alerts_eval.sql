-- name: TryAlertsEvalLock :one
SELECT pg_try_advisory_lock(25189, 3);

-- name: AlertsEvalUnlock :exec
SELECT pg_advisory_unlock(25189, 3);

-- name: TryAlertsSendLock :one
SELECT pg_try_advisory_lock(25189, 4);

-- name: AlertsSendUnlock :exec
SELECT pg_advisory_unlock(25189, 4);

-- name: AlertRuleBatch :many
-- The next enabled rules of live users after the id, with the name the user gave the pin and, for a back_online
-- rule, when the radio's gone_quiet rule last recovered and whether that rule is on.
SELECT r.id, r.user_id, r.target_kind, r.target_public_key, r.kind, r.threshold, r.threshold_high, r.state,
       r.last_sent_at, r.cursor_at, r.streak, r.version,
       COALESCE((SELECT p.name FROM user_atlas_pins p WHERE p.user_id = r.user_id AND p.public_key = r.target_public_key LIMIT 1), '')::text AS pin_name,
       g.recovered_at AS sibling_recovered_at, COALESCE(g.enabled, false)::boolean AS sibling_enabled
FROM user_alert_rules r
JOIN users u ON u.id = r.user_id AND u.disabled_at IS NULL
LEFT JOIN user_alert_rules g ON r.kind = 'back_online' AND g.user_id = r.user_id AND g.target_public_key = r.target_public_key AND g.kind = 'gone_quiet'
WHERE r.enabled AND r.id > sqlc.arg(after_id)::uuid
ORDER BY r.id
LIMIT sqlc.arg(row_limit);

-- name: AlertNodesData :many
-- polled is the same test as the overview's (AlertNodeCaps): a current polling assignment while polling is on.
SELECT public_key, COALESCE(name, '')::text AS name, last_seen, last_advert_at, device_clock_drift_seconds, prev_clock_drift_seconds,
       (sqlc.arg(polling_enabled)::boolean AND EXISTS (SELECT 1 FROM polling_assignments a WHERE a.target_public_key = nodes.public_key))::boolean AS polled
FROM nodes WHERE public_key = ANY(sqlc.arg(keys)::bytea[]);

-- name: AlertObserversData :many
-- polled is the same test as the overview's (AlertObserverCaps): status telemetry within fresh_since.
SELECT id, public_key, COALESCE(display_name, '')::text AS name, last_seen, last_status_at,
       EXISTS (SELECT 1 FROM observer_telemetry f WHERE f.observer_id = observers.id AND f.reported_at >= sqlc.arg(fresh_since)) AS polled
FROM observers WHERE public_key = ANY(sqlc.arg(keys)::bytea[]);

-- name: AlertNodeReadings :many
-- The newest trusted readings of each repeater within the window, at most 12 each.
SELECT t.k::bytea AS target_public_key, r.sampled_at, r.battery_mv, r.temperature_c, r.noise_floor, r.uptime_secs
FROM unnest(sqlc.arg(keys)::bytea[]) AS t(k)
CROSS JOIN LATERAL (
    SELECT x.sampled_at, x.battery_mv, x.temperature_c, x.noise_floor, x.uptime_secs
    FROM repeater_readings x
    WHERE x.target_public_key = t.k AND x.trusted AND x.sampled_at >= sqlc.arg(since)
    ORDER BY x.sampled_at DESC
    LIMIT 12
) r;

-- name: AlertObserverTelemetry :many
SELECT t.k::uuid AS observer_id, r.reported_at, r.battery_voltage_mv, r.noise_floor_db, r.uptime_seconds
FROM unnest(sqlc.arg(ids)::uuid[]) AS t(k)
CROSS JOIN LATERAL (
    SELECT x.reported_at, x.battery_voltage_mv, x.noise_floor_db, x.uptime_seconds
    FROM observer_telemetry x
    WHERE x.observer_id = t.k AND x.reported_at >= sqlc.arg(since)
    ORDER BY x.reported_at DESC
    LIMIT 12
) r;

-- name: AlertNodeUptimes :many
-- For each repeater and the cursor of its reboot rule: the newest trusted uptime reading at or before the cursor (the
-- baseline), then the oldest page_size readings after it, so every reading since the cursor is reached page by page.
SELECT t.k::bytea AS target_public_key, r.sampled_at, r.uptime_secs
FROM unnest(sqlc.arg(keys)::bytea[]) WITH ORDINALITY AS t(k, n)
CROSS JOIN LATERAL (SELECT (sqlc.arg(cursors)::timestamptz[])[t.n] AS c) cur
CROSS JOIN LATERAL (
    (SELECT b.sampled_at, b.uptime_secs FROM repeater_readings b
     WHERE b.target_public_key = t.k AND b.trusted AND b.uptime_secs IS NOT NULL AND b.sampled_at <= cur.c
     ORDER BY b.sampled_at DESC LIMIT 1)
    UNION ALL
    (SELECT a.sampled_at, a.uptime_secs FROM repeater_readings a
     WHERE a.target_public_key = t.k AND a.trusted AND a.uptime_secs IS NOT NULL AND a.sampled_at > cur.c
     ORDER BY a.sampled_at LIMIT sqlc.arg(page_size)::int)
) r;

-- name: AlertNodeDrifts :many
-- For each repeater and the oldest cursor of its clock rule: the advert drift at or before the cursor (the pair's
-- first half), then every kept advert after it, oldest first. The table keeps at most 16 per node.
SELECT t.k::bytea AS target_public_key, r.advert_at, r.drift_seconds
FROM unnest(sqlc.arg(keys)::bytea[]) WITH ORDINALITY AS t(k, n)
CROSS JOIN LATERAL (SELECT (sqlc.arg(cursors)::timestamptz[])[t.n] AS c) cur
CROSS JOIN LATERAL (
    (SELECT d.advert_at, d.drift_seconds FROM node_clock_drifts d JOIN nodes nn ON nn.id = d.node_id
     WHERE nn.public_key = t.k AND d.advert_at <= cur.c ORDER BY d.advert_at DESC LIMIT 1)
    UNION ALL
    (SELECT d.advert_at, d.drift_seconds FROM node_clock_drifts d JOIN nodes nn ON nn.id = d.node_id
     WHERE nn.public_key = t.k AND d.advert_at > cur.c ORDER BY d.advert_at)
) r;

-- name: AlertObserverUptimes :many
-- The same for an observer's status telemetry.
SELECT t.k::uuid AS observer_id, r.reported_at, r.uptime_seconds
FROM unnest(sqlc.arg(ids)::uuid[]) WITH ORDINALITY AS t(k, n)
CROSS JOIN LATERAL (SELECT (sqlc.arg(cursors)::timestamptz[])[t.n] AS c) cur
CROSS JOIN LATERAL (
    (SELECT b.reported_at, b.uptime_seconds FROM observer_telemetry b
     WHERE b.observer_id = t.k AND b.uptime_seconds IS NOT NULL AND b.reported_at <= cur.c
     ORDER BY b.reported_at DESC LIMIT 1)
    UNION ALL
    (SELECT a.reported_at, a.uptime_seconds FROM observer_telemetry a
     WHERE a.observer_id = t.k AND a.uptime_seconds IS NOT NULL AND a.reported_at > cur.c
     ORDER BY a.reported_at LIMIT sqlc.arg(page_size)::int)
) r;

-- name: AlertNodeDayAgo :many
-- For each repeater, the battery reading closest to 24 hours before its newest one, within 3 hours of that mark.
SELECT t.k::bytea AS target_public_key, r.sampled_at, r.battery_mv
FROM unnest(sqlc.arg(keys)::bytea[]) AS t(k)
CROSS JOIN LATERAL (
    SELECT max(x.sampled_at) AS newest FROM repeater_readings x
    WHERE x.target_public_key = t.k AND x.trusted AND x.battery_mv IS NOT NULL AND x.sampled_at >= sqlc.arg(since)
) l
CROSS JOIN LATERAL (
    SELECT y.sampled_at, y.battery_mv FROM repeater_readings y
    WHERE y.target_public_key = t.k AND y.trusted AND y.battery_mv IS NOT NULL
      AND y.sampled_at BETWEEN l.newest - interval '27 hours' AND l.newest - interval '21 hours'
    ORDER BY abs(extract(epoch FROM y.sampled_at - (l.newest - interval '24 hours')))
    LIMIT 1
) r;

-- name: AlertObserverDayAgo :many
SELECT t.k::uuid AS observer_id, r.reported_at, r.battery_voltage_mv
FROM unnest(sqlc.arg(ids)::uuid[]) AS t(k)
CROSS JOIN LATERAL (
    SELECT max(x.reported_at) AS newest FROM observer_telemetry x
    WHERE x.observer_id = t.k AND x.battery_voltage_mv IS NOT NULL AND x.reported_at >= sqlc.arg(since)
) l
CROSS JOIN LATERAL (
    SELECT y.reported_at, y.battery_voltage_mv FROM observer_telemetry y
    WHERE y.observer_id = t.k AND y.battery_voltage_mv IS NOT NULL
      AND y.reported_at BETWEEN l.newest - interval '27 hours' AND l.newest - interval '21 hours'
    ORDER BY abs(extract(epoch FROM y.reported_at - (l.newest - interval '24 hours')))
    LIMIT 1
) r;

-- name: ApplyAlertUpdates :many
-- Stores what the evaluation decided, only for rules not edited since they were read (same version). Returns the ids
-- that were stored.
UPDATE user_alert_rules r SET
    state = u.state,
    state_changed_at = CASE WHEN r.state <> u.state THEN now() ELSE r.state_changed_at END,
    last_sent_at = COALESCE(u.sent_at, r.last_sent_at),
    cursor_at = u.cursor_at,
    streak = u.streak,
    recovered_at = COALESCE(u.recovered_at, r.recovered_at)
FROM (SELECT unnest(sqlc.arg(ids)::uuid[]) AS id, unnest(sqlc.arg(versions)::bigint[]) AS version,
             unnest(sqlc.arg(states)::text[]) AS state, unnest(sqlc.arg(sent_ats)::timestamptz[]) AS sent_at,
             unnest(sqlc.arg(cursors)::timestamptz[]) AS cursor_at, unnest(sqlc.arg(streaks)::int[]) AS streak,
             unnest(sqlc.arg(recovered_ats)::timestamptz[]) AS recovered_at) u
WHERE r.id = u.id AND r.version = u.version
RETURNING r.id;

-- name: InsertAlertNotices :exec
INSERT INTO alert_outbox (user_id, pass_id, target_kind, target_public_key, name, alert_kind, value, detail)
SELECT u.user_id, sqlc.arg(pass_id)::uuid, u.target_kind, u.target_public_key, u.name, u.alert_kind, u.value, u.detail
FROM (SELECT unnest(sqlc.arg(user_ids)::uuid[]) AS user_id, unnest(sqlc.arg(target_kinds)::text[]) AS target_kind,
             unnest(sqlc.arg(target_keys)::bytea[]) AS target_public_key, unnest(sqlc.arg(names)::text[]) AS name,
             unnest(sqlc.arg(alert_kinds)::text[]) AS alert_kind, unnest(sqlc.arg(values)::bigint[]) AS value,
             unnest(sqlc.arg(details)::text[]) AS detail) u;

-- name: DueRequestAttention :many
-- Telemetry requests that need attention and were not announced at this status, locked until the transaction ends.
SELECT r.id, r.user_id, r.target_public_key, r.status, COALESCE(n.name, '')::text AS node_name
FROM telemetry_requests r
JOIN users u ON u.id = r.user_id AND u.disabled_at IS NULL
LEFT JOIN nodes n ON n.public_key = r.target_public_key
WHERE r.status IN ('login_failed', 'admin_stopped', 'disabled') AND r.attention_status IS DISTINCT FROM r.status
ORDER BY r.id
LIMIT 200
FOR UPDATE OF r SKIP LOCKED;

-- name: MarkRequestAttention :exec
UPDATE telemetry_requests t SET attention_status = u.status
FROM (SELECT unnest(sqlc.arg(ids)::uuid[]) AS id, unnest(sqlc.arg(statuses)::text[]) AS status) u
WHERE t.id = u.id;

-- name: ResetRequestAttention :exec
-- A request that is fine again can be announced again the next time it needs attention.
UPDATE telemetry_requests SET attention_status = NULL
WHERE attention_status IS NOT NULL AND status NOT IN ('login_failed', 'admin_stopped', 'disabled');
