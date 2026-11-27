-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- The clock drift of the advert before the newest one, so a clock alert can tell two consecutive adverts past its
-- limit from two that were not in a row.
ALTER TABLE nodes ADD COLUMN prev_clock_drift_seconds bigint;
