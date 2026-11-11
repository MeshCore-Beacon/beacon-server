-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Queues the last three days of rolled hours for a re-roll, which fills analytics_hourly_node_activity for them.
INSERT INTO analytics_dirty_hours (hour)
SELECT hour FROM analytics_rollup_hours
WHERE status = 'complete' AND hour >= now() - INTERVAL '72 hours'
ON CONFLICT (hour) DO NOTHING;
