-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- The newest trusted reading of a repeater by one observer, so the working-poll check does not walk readings that are
-- untrusted or by former assignees. Its own file so CONCURRENTLY runs outside a transaction. The runner rebuilds it if a
-- build was cut short. Migration 081's index stays for the lookups that do not filter on trusted.
CREATE INDEX CONCURRENTLY idx_repeater_readings_trusted_observer ON repeater_readings USING btree (target_public_key, observer_public_key, sampled_at DESC) WHERE trusted;
