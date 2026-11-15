// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
)

type outboxRow struct {
	user  uuid.UUID
	kind  string
	name  string
	pass  uuid.UUID
	value int64
}

func outbox(t *testing.T, ctx context.Context, s *Store) []outboxRow {
	t.Helper()
	rows, err := s.pool.Query(ctx, `SELECT user_id, alert_kind, name, pass_id, COALESCE(value, 0) FROM alert_outbox ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.user, &r.kind, &r.name, &r.pass, &r.value); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func ruleState(t *testing.T, ctx context.Context, s *Store, user uuid.UUID, key []byte, kind string) string {
	t.Helper()
	var st string
	if err := s.pool.QueryRow(ctx, `SELECT state FROM user_alert_rules WHERE user_id = $1 AND target_public_key = $2 AND kind = $3`, user, key, kind).Scan(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

func TestAlertEvaluationPassPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	svc := alertSvc(s)
	now := time.Now().UTC().Truncate(time.Second)
	alice, bob, carol := newUser(t, ctx, s, "alice"), newUser(t, ctx, s, "bob"), newUser(t, ctx, s, "carol")

	rep, obsKey := atlasKey(1), atlasKey(2)
	insertNode(t, ctx, s, rep, 2, "Hilltop\r\nBcc: evil@example.org")
	if _, err := pool.Exec(ctx, `UPDATE nodes SET last_seen = $2 WHERE public_key = $1`, rep, now.Add(-30*time.Hour)); err != nil {
		t.Fatal(err)
	}
	insertReading(t, ctx, s, rep, now.Add(-45*time.Hour), 3450, nil)
	insertReading(t, ctx, s, rep, now.Add(-44*time.Hour), 3400, nil)
	var obsID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO observers (public_key, display_name, last_seen, last_status_at) VALUES ($1, 'North', $2, $3) RETURNING id`,
		obsKey, now, now.Add(-3*time.Hour)).Scan(&obsID); err != nil {
		t.Fatal(err)
	}

	for _, u := range []struct{ id uuid.UUID }{{alice.ID}, {bob.ID}, {carol.ID}} {
		pinAtlas(t, ctx, s, mustUser(t, ctx, s, u.id), 0, api.StoredPin{Kind: "node", Key: rep, Name: "My hill"}, api.StoredPin{Kind: "observer", Key: obsKey})
	}
	put := func(u uuid.UUID, kind, tk string, key []byte, th *int) {
		t.Helper()
		if _, err := svc.PutRule(ctx, u, alerts.RuleInput{TargetKind: tk, PublicKey: key, Kind: kind, Threshold: th, Enabled: true}); err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
	}
	put(alice.ID, "gone_quiet", "node", rep, ipp(12))
	put(alice.ID, "back_online", "node", rep, nil)
	put(alice.ID, "battery_low", "node", rep, ipp(3500))
	put(bob.ID, "gone_quiet", "node", rep, ipp(48))
	put(bob.ID, "battery_low", "node", rep, ipp(3000))
	put(bob.ID, "observer_offline", "observer", obsKey, ipp(60))
	put(carol.ID, "gone_quiet", "node", rep, ipp(12))
	if err := s.SetUserDisabled(ctx, carol.ID, &now); err != nil {
		t.Fatal(err)
	}
	// A request of Alice's that needs attention.
	if _, err := pool.Exec(ctx, `INSERT INTO telemetry_requests (id, user_id, target_public_key, status, created_at, updated_at, enabled_at)
		VALUES (gen_random_uuid(), $1, $2, 'login_failed', now(), now(), now())`, alice.ID, rep); err != nil {
		t.Fatal(err)
	}
	// The rules were just made: judge data only from then, so push the cursors back like an old rule.
	if _, err := pool.Exec(ctx, `UPDATE user_alert_rules SET cursor_at = now() - interval '7 days'`); err != nil {
		t.Fatal(err)
	}

	task := alerts.NewTask(AlertEval{s}, time.Now)
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	got := outbox(t, ctx, s)
	byUser := map[uuid.UUID][]string{}
	for _, r := range got {
		byUser[r.user] = append(byUser[r.user], r.kind)
	}
	if len(got) != 4 || len(byUser[alice.ID]) != 3 || len(byUser[bob.ID]) != 1 || byUser[bob.ID][0] != "observer_offline" || len(byUser[carol.ID]) != 0 {
		t.Fatalf("outbox %+v", got)
	}
	var alicePass uuid.UUID
	for _, r := range got {
		if r.user == alice.ID {
			if alicePass != uuid.Nil && r.pass != alicePass {
				t.Fatal("one pass, several pass ids")
			}
			alicePass = r.pass
			if r.name != "Hilltop" && r.name != "My hill" && r.kind != "request_attention" {
				t.Fatalf("name %q", r.name)
			}
		}
		for _, c := range r.name {
			if c == '\r' || c == '\n' {
				t.Fatalf("control character in %q", r.name)
			}
		}
	}
	for kind, want := range map[string]string{"gone_quiet": "firing", "battery_low": "firing", "back_online": "ok"} {
		if st := ruleState(t, ctx, s, alice.ID, rep, kind); st != want {
			t.Errorf("alice %s is %s, want %s", kind, st, want)
		}
	}
	if st := ruleState(t, ctx, s, bob.ID, rep, "gone_quiet"); st != "ok" {
		t.Errorf("bob gone_quiet %s", st)
	}
	if st := ruleState(t, ctx, s, bob.ID, rep, "battery_low"); st != "ok" {
		t.Errorf("bob battery_low %s", st)
	}
	if st := ruleState(t, ctx, s, carol.ID, rep, "gone_quiet"); st != "unknown" {
		t.Errorf("a disabled user's rule was evaluated: %s", st)
	}
	var sent *time.Time
	if err := pool.QueryRow(ctx, `SELECT last_sent_at FROM user_alert_rules WHERE user_id = $1 AND kind = 'gone_quiet'`, alice.ID).Scan(&sent); err != nil || sent == nil {
		t.Fatalf("last_sent_at %v %v", sent, err)
	}

	// A second pass changes nothing: no duplicates.
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(outbox(t, ctx, s)); n != 4 {
		t.Fatalf("%d outbox rows after a second pass", n)
	}

	// The repeater is heard again: gone_quiet recovers quietly, then back_online announces it once.
	if _, err := pool.Exec(ctx, `UPDATE nodes SET last_seen = now() WHERE public_key = $1`, rep); err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if st := ruleState(t, ctx, s, alice.ID, rep, "gone_quiet"); st != "ok" {
		t.Fatalf("gone_quiet %s after the radio was heard", st)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	after := outbox(t, ctx, s)
	if len(after) != 5 || after[4].kind != "back_online" || after[4].user != alice.ID {
		t.Fatalf("outbox %+v", after)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(outbox(t, ctx, s)); n != 5 {
		t.Fatalf("back_online announced again: %d", n)
	}

	// Rules the user turned off are not evaluated, and cascade with the account.
	if err := s.DeleteUser(ctx, bob.ID); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM alert_outbox WHERE user_id = $1`, bob.ID).Scan(&n); err != nil || n != 0 {
		t.Fatalf("outbox rows of a deleted user: %d %v", n, err)
	}
}

func mustUser(t *testing.T, ctx context.Context, s *Store, id uuid.UUID) users.User {
	t.Helper()
	u, err := s.UserByID(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestAlertEvaluationGuardPostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	a := AlertEval{s}
	l1, ok, err := a.BeginEvalLock(ctx)
	if err != nil || !ok {
		t.Fatalf("first lock: %v %v", ok, err)
	}
	if _, ok, err := a.BeginEvalLock(ctx); err != nil || ok {
		t.Fatalf("second lock while held: ok=%v err=%v", ok, err)
	}
	l1.Close(ctx)
	l3, ok, err := a.BeginEvalLock(ctx)
	if err != nil || !ok {
		t.Fatalf("lock after release: %v %v", ok, err)
	}
	l3.Close(ctx)
}

func TestAlertApplySkipsEditedRulesPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	svc := alertSvc(s)
	u := newUser(t, ctx, s, "alice")
	key := atlasKey(1)
	insertNode(t, ctx, s, key, 2, "Hilltop")
	pinAtlas(t, ctx, s, u, 0, api.StoredPin{Kind: "node", Key: key})
	rule, err := svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: key, Kind: "gone_quiet", Threshold: ipp(12), Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	a := AlertEval{s}
	rows, err := a.RuleBatch(ctx, uuid.Nil, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("%v %d", err, len(rows))
	}
	// The user edits the rule after the pass read it.
	if _, err := svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: key, Kind: "gone_quiet", Threshold: ipp(24), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	err = a.Apply(ctx, uuid.New(),
		[]alerts.Update{{ID: rule.ID, Version: rows[0].Version, State: alerts.StateFiring, SentAt: now, Cursor: now}},
		[]alerts.Notice{{RuleID: rule.ID, UserID: u.ID, TargetKind: "node", Key: key, Name: "Hilltop", Kind: "gone_quiet", Value: 30}})
	if err != nil {
		t.Fatal(err)
	}
	var st string
	var n int
	_ = pool.QueryRow(ctx, `SELECT state FROM user_alert_rules WHERE id = $1`, rule.ID).Scan(&st)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM alert_outbox`).Scan(&n)
	if st != "unknown" || n != 0 {
		t.Fatalf("a stale evaluation overwrote an edit: state %s, %d notices", st, n)
	}
}
