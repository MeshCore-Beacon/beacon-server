// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"time"
)

const usersCleanupInterval = time.Hour

var usersNow = time.Now

// UsersMaintStore is what the account cleanup needs; *db.Store satisfies it.
type UsersMaintStore interface {
	DeleteExpiredAuth(ctx context.Context, now time.Time) error
}

// UsersTasks removes ended sessions, spent or expired one-time tokens and stale unconfirmed sign-ups every hour.
func UsersTasks(store UsersMaintStore) []Task {
	return []Task{{Name: "users_cleanup", Interval: usersCleanupInterval, Run: func(ctx context.Context) error {
		return store.DeleteExpiredAuth(ctx, usersNow())
	}}}
}
