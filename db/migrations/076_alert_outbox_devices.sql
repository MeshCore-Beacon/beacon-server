-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- The devices a push notice has reached, so a retry goes only to the ones that did not get it.
CREATE TABLE alert_outbox_devices (
    outbox_id bigint NOT NULL REFERENCES alert_outbox(id) ON DELETE CASCADE,
    session_id uuid NOT NULL,
    delivered_at timestamp with time zone NOT NULL,
    PRIMARY KEY (outbox_id, session_id)
);
