-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Email and push retry on their own: a failing channel no longer holds back or drops the other.
-- A channel is done once it went out, was not needed, or ran out of attempts; a notice is finished when both are.
ALTER TABLE alert_outbox
    ADD COLUMN email_attempts smallint NOT NULL DEFAULT 0,
    ADD COLUMN email_next_attempt_at timestamp with time zone,
    ADD COLUMN email_done_at timestamp with time zone,
    ADD COLUMN push_attempts smallint NOT NULL DEFAULT 0,
    ADD COLUMN push_next_attempt_at timestamp with time zone,
    ADD COLUMN push_done_at timestamp with time zone;

-- The shared count and hold may all have come from push, so unsent mail starts with none and is due at once.
UPDATE alert_outbox SET push_attempts = attempts, push_next_attempt_at = next_attempt_at, email_done_at = email_sent_at
WHERE delivered_at IS NULL;
UPDATE alert_outbox SET email_done_at = delivered_at, push_done_at = delivered_at WHERE delivered_at IS NOT NULL;

ALTER TABLE alert_outbox DROP COLUMN attempts, DROP COLUMN next_attempt_at;
