-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- One row per (channel message, site that heard it); region message pages read this instead of every observation.
CREATE TABLE channel_message_iatas (
    message_id bigint NOT NULL REFERENCES channel_messages(id) ON DELETE CASCADE,
    iata character(3) NOT NULL,
    channel_id integer NOT NULL,
    scope_id integer,
    PRIMARY KEY (message_id, iata)
);
