// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
	"github.com/google/uuid"
)

// A reboot that is older than the newest twelve readings is still newer than the cursor, so it is still found.
func TestAlertRebootBeyondTwelveReadingsPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	u := newUser(t, ctx, s, "alice")
	now := time.Now()
	key := atlasKey(1)
	var obs uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO observers (public_key, display_name, last_seen, last_status_at) VALUES ($1, 'North', $2, $2) RETURNING id`, key, now).Scan(&obs); err != nil {
		t.Fatal(err)
	}
	// Judged before the last pass: uptime 10000. The restart came next, then twelve ordinary samples.
	for i := 0; i < 14; i++ {
		up := int64(i * 60)
		if i == 0 {
			up = 10000
		}
		if _, err := pool.Exec(ctx, `INSERT INTO observer_telemetry (id, observer_id, reported_at, uptime_seconds) VALUES ($1, $2, $3, $4)`,
			i+1, obs, now.Add(time.Duration(i-13)*time.Minute), up); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO user_alert_rules (user_id, target_kind, target_public_key, kind, state, cursor_at)
		VALUES ($1, 'observer', $2, 'rebooted', 'ok', $3)`, u.ID, key, now.Add(-13*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := alerts.NewTask(AlertEval{s}, time.Now).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(outbox(t, ctx, s)); n != 1 {
		t.Fatalf("%d notices for a reboot among thirteen unread readings, want 1", n)
	}
}
