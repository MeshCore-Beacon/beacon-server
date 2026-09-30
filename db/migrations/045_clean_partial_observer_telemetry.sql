-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Some /status messages carry uptime but no radio stats. Those rows zero the airtime and
-- noise floor, dragging bucket averages down; ingest now skips them (statusStats.usable).
DELETE FROM observer_telemetry
WHERE noise_floor_db = 0 AND airtime_tx_secs = 0 AND airtime_rx_secs = 0;
