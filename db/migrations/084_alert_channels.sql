-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Each user picks the channels alerts reach them by.
ALTER TABLE users ADD COLUMN alert_email boolean NOT NULL DEFAULT true;
ALTER TABLE users ADD COLUMN alert_push boolean NOT NULL DEFAULT true;

-- When a notice's mail went out, so retrying its push never sends the mail twice.
ALTER TABLE alert_outbox ADD COLUMN email_sent_at timestamp with time zone;

-- Push goes through the push server now: a device holds its handle there and the key its notices are sealed with.
-- Devices registered before have neither, so the apps register again.
DELETE FROM user_devices;
ALTER TABLE user_devices ADD COLUMN payload_key bytea NOT NULL CHECK (octet_length(payload_key) = 32);
