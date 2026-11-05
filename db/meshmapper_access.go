// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
)

// PrepareRepeaterAccess invalidates imported profiles atomically when access changes.
// No raw traffic, telemetry, routes, or next-attempt cooldowns are changed.
func (s *Store) PrepareRepeaterAccess(ctx context.Context, fingerprint string) (bool, error) {
	return prepareRepeaterAccess(ctx, s.pool, fingerprint)
}

func prepareRepeaterAccess(ctx context.Context, q sqlc.DBTX, fingerprint string) (bool, error) {
	var changed bool
	err := q.QueryRow(ctx, `WITH changed AS (
 INSERT INTO meshmapper_repeater_access(singleton, fingerprint) VALUES(true, $1)
 ON CONFLICT (singleton) DO UPDATE SET fingerprint = EXCLUDED.fingerprint
 WHERE meshmapper_repeater_access.fingerprint IS DISTINCT FROM EXCLUDED.fingerprint RETURNING 1
), cleared AS (
 DELETE FROM meshmapper_repeaters WHERE EXISTS (SELECT 1 FROM changed) RETURNING 1
), reset AS (
 UPDATE meshmapper_repeater_sources SET etag = NULL, checked_at = NULL, last_error = 'access changed'
 WHERE EXISTS (SELECT 1 FROM changed) RETURNING 1
)
SELECT EXISTS (SELECT 1 FROM cleared)`, fingerprint).Scan(&changed)
	return changed, err
}
