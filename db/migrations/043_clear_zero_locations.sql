-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- An explicit 0/0 advert now clears the stored position; drop old 0/0 rows and stale sources.
UPDATE nodes SET latitude = NULL, longitude = NULL, location_source = NULL
WHERE location_source = 'advert' AND (latitude IS NULL OR (latitude = 0 AND longitude = 0));
