-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- name: GetObserverTelemetrySince :many
SELECT id, reported_at, battery_voltage_mv, airtime_tx_secs, airtime_rx_secs,
       noise_floor_db, uptime_seconds, queue_length, debug_flags, receive_errors
FROM observer_telemetry
WHERE observer_id = $1
  AND reported_at >= $2::timestamptz
  AND id > $3::bigint
ORDER BY reported_at ASC
