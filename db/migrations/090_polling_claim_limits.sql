-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- What a poller says about itself: how many targets it can hold and how often it polls each, in minutes.
-- NULL when its firmware does not report the field.
ALTER TABLE polling_claims ADD COLUMN IF NOT EXISTS max_slots smallint;
ALTER TABLE polling_claims ADD COLUMN IF NOT EXISTS poll_interval integer;
