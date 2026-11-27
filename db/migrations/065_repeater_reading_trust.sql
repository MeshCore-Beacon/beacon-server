-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- A reading is trusted when its observer held an assignment with advert evidence for the target at the time it was
-- stored, so charts do not shift when assignments change. Existing rows use the assignments as they stand now.
ALTER TABLE repeater_readings ADD COLUMN trusted boolean NOT NULL DEFAULT false;

UPDATE repeater_readings r SET trusted = true
WHERE EXISTS (
    SELECT 1 FROM polling_assignments a
    WHERE a.target_public_key = r.target_public_key AND a.observer_public_key = r.observer_public_key AND a.evidence_count > 0
);
