-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- A notice whose delivery failed waits until this time before the next attempt.
ALTER TABLE alert_outbox ADD COLUMN next_attempt_at timestamp with time zone;
