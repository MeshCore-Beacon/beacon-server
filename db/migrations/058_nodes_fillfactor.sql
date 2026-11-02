-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Room on each page for HOT updates of every node advert's last_seen write, as 024 gives observers and channels.
ALTER TABLE nodes SET (fillfactor = 50);
