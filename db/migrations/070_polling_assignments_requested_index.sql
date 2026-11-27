-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- The seat count of an observer looks up its requested assignments.
CREATE INDEX idx_polling_assignments_observer_requested ON polling_assignments USING btree (observer_public_key) WHERE requested;
