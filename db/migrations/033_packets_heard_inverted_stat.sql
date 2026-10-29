-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later

-- Without these statistics the planner assumes a third of packets are inverted and misprices the window's second branch.
CREATE STATISTICS packets_heard_inverted_stat ON ((first_heard_at > last_heard_at)) FROM packets;
ANALYZE packets;
