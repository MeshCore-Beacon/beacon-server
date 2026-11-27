-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- One row per evaluation pass: finished_at is written when the pass ends. The sender takes the notices of a finished
-- pass only, so one pass's notices for a user are never split across mails; a pass that never finished (the process
-- stopped) is released after a timeout. Notices of a pass with no row here are sendable.
CREATE TABLE alert_passes (
    pass_id uuid PRIMARY KEY,
    started_at timestamp with time zone NOT NULL DEFAULT now(),
    finished_at timestamp with time zone
);
