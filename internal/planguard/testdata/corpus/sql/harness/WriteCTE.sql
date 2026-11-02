-- Copyright 2026 Beacon Contributors
-- SPDX-License-Identifier: AGPL-3.0-or-later
-- A SELECT that deletes: kind select must be refused.
WITH d AS (DELETE FROM known_routes RETURNING 1) SELECT count(*) FROM d
