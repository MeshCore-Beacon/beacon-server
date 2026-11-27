-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Readings of a repeater an observer polls on its own are trusted without advert evidence, as long as no
-- assignee of that repeater has any. Stored readings follow the assignments as they stand now.
UPDATE repeater_readings r SET trusted = true
WHERE NOT r.trusted AND EXISTS (
    SELECT 1 FROM polling_assignments a
    WHERE a.target_public_key = r.target_public_key AND a.observer_public_key = r.observer_public_key
      AND NOT a.requested
      AND NOT EXISTS (
          SELECT 1 FROM polling_assignments e
          WHERE e.target_public_key = a.target_public_key AND e.evidence_count > 0)
);
