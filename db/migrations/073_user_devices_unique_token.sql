-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- A push token belongs to one session: keep the newest registration of any token held twice, then let the index
-- enforce it.
DELETE FROM user_devices d
USING user_devices n
WHERE d.push_token = n.push_token
  AND (n.updated_at, n.session_id) > (d.updated_at, d.session_id);

DROP INDEX idx_user_devices_token;
CREATE UNIQUE INDEX idx_user_devices_token ON user_devices USING btree (push_token);
