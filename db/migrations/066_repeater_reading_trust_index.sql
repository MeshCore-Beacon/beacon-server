-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- The trusted-readings index for the node telemetry charts. Its own file so CONCURRENTLY runs outside a transaction
-- and the build does not block ingest writes to repeater_readings. The runner rebuilds it if an earlier attempt left it invalid.
CREATE INDEX CONCURRENTLY idx_repeater_readings_trusted ON repeater_readings USING btree (target_public_key, sampled_at DESC) WHERE trusted;
