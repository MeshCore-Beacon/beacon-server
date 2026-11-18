// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
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

// Two adverts past the limit in a row still count when a normal one follows before the next pass.
func TestAlertClockBreachBetweenPassesPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	svc := alertSvc(s)
	u := newUser(t, ctx, s, "alice")
	key := atlasKey(1)
	advertWithDrift(t, ctx, s, key, 0)
	pinAtlas(t, ctx, s, u, 0, api.StoredPin{Kind: "node", Key: key})
	if _, err := svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: key, Kind: "clock", Threshold: ipp(300), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE user_alert_rules SET cursor_at = now() - interval '1 day', state = 'ok'`); err != nil {
		t.Fatal(err)
	}
	for _, d := range []int64{600, 600, 0} {
		advertWithDrift(t, ctx, s, key, d)
		time.Sleep(5 * time.Millisecond)
	}
	task := alerts.NewTask(AlertEval{s}, time.Now)
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(outbox(t, ctx, s)); n != 1 {
		t.Fatalf("600, 600, 0 since the last pass: %d notices, want 1", n)
	}
	if got := ruleStateOf(t, ctx, pool, "clock"); got != "ok" {
		t.Fatalf("state after the recovery: %s", got)
	}
	// Nothing new: no second notice.
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(outbox(t, ctx, s)); n != 1 {
		t.Fatalf("the breach was announced again: %d notices", n)
	}
}

// With polling switched off the assignment rows that remain are no current source: repeater telemetry kinds are
// unavailable (not_polled) and their rules read inactive. Observer status alerts do not depend on it.
func TestAlertTelemetryStopsWhenPollingIsOffPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	svc := alertSvc(s)
	u := newUser(t, ctx, s, "alice")
	rep := atlasKey(1)
	insertNode(t, ctx, s, rep, 2, "Hilltop")
	insertReading(t, ctx, s, rep, time.Now().Add(-10*time.Minute), 3000, nil)
	insertReading(t, ctx, s, rep, time.Now().Add(-20*time.Minute), 3000, nil)
	assignPoller(t, ctx, s, rep)
	pinAtlas(t, ctx, s, u, 0, api.StoredPin{Kind: "node", Key: rep})
	if _, err := pool.Exec(ctx, `INSERT INTO user_alert_rules (user_id, target_kind, target_public_key, kind, threshold, cursor_at)
		VALUES ($1, 'node', $2, 'battery_low', 3500, now() - interval '3 days')`, u.ID, rep); err != nil {
		t.Fatal(err)
	}
	task := alerts.NewTask(AlertEval{s}, time.Now)
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got := ruleStateOf(t, ctx, pool, "battery_low"); got != "firing" {
		t.Fatalf("with polling on: %s", got)
	}
	s.SetPollingEnabled(false)
	ov, err := svc.Overview(ctx, u.ID)
	if err != nil || len(ov.Targets) != 1 || ov.Targets[0].Caps.Polled {
		t.Fatalf("still polled with polling off: %+v %v", ov, err)
	}
	if got := ov.Targets[0].UnavailableCodes["battery_low"]; got != alerts.CodeNotPolled {
		t.Fatalf("battery_low with polling off: code %q, available %v", got, ov.Targets[0].Available)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got := ruleStateOf(t, ctx, pool, "battery_low"); got != "inactive" {
		t.Fatalf("rule with polling off: %s", got)
	}
}

// The sender takes the notices of a finished pass only, so a pass that is still being evaluated is not split across
// mails; one that never finished is released after the timeout.
func TestAlertMailWaitsForTheFinishedPassPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	u := newUser(t, ctx, s, "alice")
	if err := s.SetUserEmailVerified(ctx, u.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	pass := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO alert_passes (pass_id) VALUES ($1)`, pass); err != nil {
		t.Fatal(err)
	}
	m := &recMail{}
	sender := alerts.NewSender(AlertEval{s}, alerts.SenderConfig{Mail: m})
	queueNotice(t, ctx, s, u.ID, pass, "battery_low", "Hill", time.Now())
	if err := sender.Run(ctx); err != nil {
		t.Fatal(err)
	}
	queueNotice(t, ctx, s, u.ID, pass, "rebooted", "Hill", time.Now())
	if err := sender.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(m.got) != 0 {
		t.Fatalf("a pass still being evaluated was sent: %v", m.got)
	}
	if _, err := pool.Exec(ctx, `UPDATE alert_passes SET finished_at = now() WHERE pass_id = $1`, pass); err != nil {
		t.Fatal(err)
	}
	if err := sender.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(m.got) != 1 || !strings.Contains(m.got[0], "2 alerts") {
		t.Fatalf("a finished pass should go out as one mail: %v", m.got)
	}
	// A pass the process never finished is released after the timeout.
	stuck := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO alert_passes (pass_id, started_at) VALUES ($1, now() - interval '16 minutes')`, stuck); err != nil {
		t.Fatal(err)
	}
	queueNotice(t, ctx, s, u.ID, stuck, "rebooted", "Hill", time.Now())
	if err := sender.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(m.got) != 2 {
		t.Fatalf("an interrupted pass was never released: %v", m.got)
	}
}
