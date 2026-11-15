-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- The newest reading of a repeater by one observer, so the latest-reading lookups skip readings by former assignees.
-- Its own file so CONCURRENTLY runs outside a transaction. The runner rebuilds it if a build was cut short.
CREATE INDEX CONCURRENTLY idx_repeater_readings_target_observer ON repeater_readings USING btree (target_public_key, observer_public_key, sampled_at DESC);
