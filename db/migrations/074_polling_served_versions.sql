-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- The password version (ver) last served to an observer for a requested repeater, so a state report from that
-- observer is judged against the password it was given, not whichever one is in use when the report arrives.
CREATE TABLE polling_served_versions (
    target_public_key bytea NOT NULL,
    observer_public_key bytea NOT NULL,
    ver bigint NOT NULL,
    served_at timestamp with time zone NOT NULL,
    PRIMARY KEY (target_public_key, observer_public_key)
);
