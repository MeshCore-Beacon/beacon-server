-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Room on each page for HOT updates of the hottest rows.
ALTER TABLE observers SET (fillfactor = 50);
ALTER TABLE channels SET (fillfactor = 50);
