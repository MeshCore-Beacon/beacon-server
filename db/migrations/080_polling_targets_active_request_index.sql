-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Deleting a request clears active_request_id through ON DELETE SET NULL; without this it scans every target ever
-- requested. Its own file so CONCURRENTLY runs outside a transaction. The runner rebuilds it if a build was cut short.
CREATE INDEX CONCURRENTLY idx_polling_targets_active_request ON polling_targets USING btree (active_request_id) WHERE active_request_id IS NOT NULL;
