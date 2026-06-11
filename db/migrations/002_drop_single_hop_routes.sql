-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: agpl

-- 002_drop_single_hop_routes.sql
-- routes were being stored as long as the path was
-- greater than 0.
DELETE FROM known_routes WHERE hop_count < 2;
