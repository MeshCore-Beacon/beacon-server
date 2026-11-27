-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- The clock drift of each node's last 16 adverts, so a clock alert sees every advert since its last pass, not only
-- the newest two.
CREATE TABLE node_clock_drifts (
    node_id uuid NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
    advert_at timestamp with time zone NOT NULL,
    drift_seconds bigint NOT NULL,
    PRIMARY KEY (node_id, advert_at)
);

-- Each node starts with the advert it has now.
INSERT INTO node_clock_drifts (node_id, advert_at, drift_seconds)
SELECT id, last_advert_at, device_clock_drift_seconds FROM nodes
WHERE device_clock_drift_seconds IS NOT NULL AND last_advert_at IS NOT NULL;
