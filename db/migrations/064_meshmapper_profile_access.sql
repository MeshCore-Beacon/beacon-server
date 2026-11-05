-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Private cache binding, never an API key. A changed key/import area invalidates
-- previously authorized profiles without resetting provider cooldowns.
CREATE TABLE IF NOT EXISTS meshmapper_repeater_access (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    fingerprint text NOT NULL
);
