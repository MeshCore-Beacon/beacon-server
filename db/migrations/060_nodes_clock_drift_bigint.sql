-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- An advert's clock can be up to about 2.5e9 s ahead (a uint32 timestamp), past integer. Rewrites nodes once.
ALTER TABLE nodes ALTER COLUMN device_clock_drift_seconds TYPE bigint;
