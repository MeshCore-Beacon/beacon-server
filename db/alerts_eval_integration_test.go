// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	mailer "github.com/MeshCore-Beacon/beacon-server/internal/mail"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
	assignPoller(t, ctx, s, rep)
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

type recMail struct {
	got  []string
	fail error
}

func (m *recMail) Send(_ context.Context, msg mailer.Message) error {
	if m.fail != nil {
		return m.fail
	}
	m.got = append(m.got, msg.To+"|"+msg.Subject+"|"+msg.Text)
	return nil
}

type recPush struct {
	sent []alerts.PushMessage
	gone map[string]bool
}

func (p *recPush) Send(_ context.Context, msgs []alerts.PushMessage) ([]alerts.PushResult, error) {
	out := make([]alerts.PushResult, len(msgs))
	for i, m := range msgs {
		if p.gone[m.Handle] {
			out[i].Status = alerts.PushGone
			continue
		}
		p.sent = append(p.sent, m)
	}
	return out, nil
}

// openNotice opens a notice sealed with testPayloadKey.
func openNotice(t *testing.T, ct []byte) map[string]any {
	t.Helper()
	block, _ := aes.NewCipher(make([]byte, alerts.PayloadKeySize))
	gcm, _ := cipher.NewGCM(block)
	pt, err := gcm.Open(nil, ct[:12], ct[12:], []byte("beacon-push/v1:inst_test"))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(pt, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func queueNotice(t *testing.T, ctx context.Context, s *Store, user uuid.UUID, pass uuid.UUID, kind, name string, created time.Time) {
	t.Helper()
	if _, err := s.pool.Exec(ctx, `INSERT INTO alert_outbox (user_id, pass_id, target_kind, target_public_key, name, alert_kind, value, created_at)
		VALUES ($1, $2, 'node', $3, $4, $5, 30, $6)`, user, pass, atlasKey(1), name, kind, created); err != nil {
		t.Fatal(err)
	}
}

func TestAlertEmailDeliveryPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	alice := newUser(t, ctx, s, "alice")
	if err := s.SetUserEmailVerified(ctx, alice.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	bob := newUser(t, ctx, s, "bob") // address not verified
	pass := uuid.New()
	now := time.Now()
	queueNotice(t, ctx, s, alice.ID, pass, "gone_quiet", "Hilltop", now)
	queueNotice(t, ctx, s, alice.ID, pass, "rebooted", "Hilltop", now)
	queueNotice(t, ctx, s, alice.ID, uuid.New(), "rebooted", "Old news", now.Add(-3*time.Hour)) // older than two hours: dropped
	queueNotice(t, ctx, s, bob.ID, pass, "rebooted", "Hilltop", now)
	pu, _ := url.Parse("https://beacon.example.org")
	m := &recMail{}
	sender := alerts.NewSender(AlertEval{s}, alerts.SenderConfig{Mail: m, PublicURL: pu})
	if err := sender.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(m.got) != 1 || !strings.HasPrefix(m.got[0], alice.Email+"|Beacon: 2 alerts|") || strings.Contains(m.got[0], "Old news") {
		t.Fatalf("%q", m.got)
	}
	var pending, total int
	_ = pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE delivered_at IS NULL), count(*) FROM alert_outbox`).Scan(&pending, &total)
	if pending != 0 || total != 3 { // the expired row is gone, the others marked delivered (Bob has no verified address)
		t.Fatalf("pending %d total %d", pending, total)
	}
	// Nothing is sent twice.
	if err := sender.Run(ctx); err != nil || len(m.got) != 1 {
		t.Fatalf("second run: %v, %d mails", err, len(m.got))
	}
}

func TestAlertPushDeliveryPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	alice := newUser(t, ctx, s, "alice")
	live, ended := newSession(t, ctx, s, alice, "app", 1), newSession(t, ctx, s, alice, "app", 2)
	svc := alertSvc(s)
	if err := svc.RegisterDevice(ctx, alice.ID, live.ID, "ios", "h_live-token", testPayloadKey); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterDevice(ctx, alice.ID, ended.ID, "android", "h_ended-token", testPayloadKey); err != nil {
		t.Fatal(err)
	}
	if err := s.EndSession(ctx, alice.ID, ended.ID); err != nil {
		t.Fatal(err)
	}
	insertNode(t, ctx, s, atlasKey(1), 2, "Hilltop")
	queueNotice(t, ctx, s, alice.ID, uuid.New(), "battery_low", "Hilltop", time.Now())
	p := &recPush{}
	if err := alerts.NewSender(AlertEval{s}, alerts.SenderConfig{Push: p, PushID: "inst_test"}).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(p.sent) != 1 || p.sent[0].Handle != "h_live-token" {
		t.Fatalf("%+v", p.sent)
	}
	got := openNotice(t, p.sent[0].Ciphertext)
	if got["targetKind"] != "node" || got["publicKey"] != hex.EncodeToString(atlasKey(1)) || got["kind"] != "battery_low" || got["value"] != float64(30) ||
		got["id"] != nodeIDOf(t, ctx, s, atlasKey(1)).String() {
		t.Fatalf("notice: %v", got)
	}

	// An unregistered token is deleted, and a notice no device took is retried until it is dropped.
	queueNotice(t, ctx, s, alice.ID, uuid.New(), "rebooted", "Hilltop", time.Now())
	p = &recPush{gone: map[string]bool{"h_live-token": true}}
	if err := alerts.NewSender(AlertEval{s}, alerts.SenderConfig{Push: p, PushID: "inst_test"}).Run(ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM user_devices WHERE push_token = 'h_live-token'`).Scan(&n)
	if n != 0 {
		t.Fatal("the unregistered token was kept")
	}
}

func TestAlertOutboxRetriesThenDropsPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	alice := newUser(t, ctx, s, "alice")
	sess := newSession(t, ctx, s, alice, "app", 1)
	if err := alertSvc(s).RegisterDevice(ctx, alice.ID, sess.ID, "ios", "h_flaky", testPayloadKey); err != nil {
		t.Fatal(err)
	}
	queueNotice(t, ctx, s, alice.ID, uuid.New(), "rebooted", "Hilltop", time.Now())
	p := &failPush{}
	// Each failure holds the notice back, so the clock moves on between runs.
	at := time.Now()
	sender := alerts.NewSender(AlertEval{s}, alerts.SenderConfig{Push: p, PushID: "inst_test", Now: func() time.Time { return at }})
	for i := 0; i < alerts.MaxAttempts; i++ {
		if err := sender.Run(ctx); err != nil {
			t.Fatal(err)
		}
		at = at.Add(10 * time.Minute)
	}
	var n int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM alert_outbox`).Scan(&n)
	if n != 0 || p.calls != alerts.MaxAttempts {
		t.Fatalf("%d rows left after %d attempts", n, p.calls)
	}
}

type failPush struct{ calls int }

func (p *failPush) Send(context.Context, []alerts.PushMessage) ([]alerts.PushResult, error) {
	p.calls++
	return nil, &alerts.PushError{Status: 500}
}

func TestAlertSendGuardPostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	a := AlertEval{s}
	l1, ok, err := a.BeginSendLock(ctx)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if _, ok, _ := a.BeginSendLock(ctx); ok {
		t.Fatal("two senders")
	}
	// The send guard is independent of the evaluation guard.
	l2, ok, err := a.BeginEvalLock(ctx)
	if err != nil || !ok {
		t.Fatalf("eval guard blocked by the send guard: %v", err)
	}
	l2.Close(ctx)
	l1.Close(ctx)
}

func TestAlertBackOnlineNeedsEnabledGoneQuietPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	svc := alertSvc(s)
	alice := newUser(t, ctx, s, "alice")
	rep := atlasKey(1)
	insertNode(t, ctx, s, rep, 2, "Hilltop")
	pinAtlas(t, ctx, s, alice, 0, api.StoredPin{Kind: "node", Key: rep})
	put := func(kind string, th *int, enabled bool) {
		t.Helper()
		if _, err := svc.PutRule(ctx, alice.ID, alerts.RuleInput{TargetKind: "node", PublicKey: rep, Kind: kind, Threshold: th, Enabled: enabled}); err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
	}
	put("gone_quiet", ipp(12), true)
	put("back_online", nil, true)
	run := func() {
		t.Helper()
		if err := alerts.NewTask(AlertEval{s}, time.Now).Run(ctx); err != nil {
			t.Fatal(err)
		}
	}
	run()
	if st := ruleState(t, ctx, s, alice.ID, rep, "back_online"); st != "ok" {
		t.Fatalf("with its partner on: %s", st)
	}
	// The partner is switched off: back_online reads inactive. Off, not deleted.
	put("gone_quiet", ipp(12), false)
	run()
	if st := ruleState(t, ctx, s, alice.ID, rep, "back_online"); st != "inactive" {
		t.Fatalf("partner off: %s", st)
	}
	// Back on, it reads again; deleted, it is inactive again.
	put("gone_quiet", ipp(12), true)
	run()
	if st := ruleState(t, ctx, s, alice.ID, rep, "back_online"); st != "ok" {
		t.Fatalf("partner back on: %s", st)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM user_alert_rules WHERE user_id = $1 AND kind = 'gone_quiet'`, alice.ID); err != nil {
		t.Fatal(err)
	}
	run()
	if st := ruleState(t, ctx, s, alice.ID, rep, "back_online"); st != "inactive" {
		t.Fatalf("partner deleted: %s", st)
	}
}

// An email the SMTP server did not take stays in the outbox: pending, an attempt counted, held back for a delay,
// and sent once the delay has passed and the server answers.
func TestAlertEmailStaysPendingUntilSentPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	alice := newUser(t, ctx, s, "alice")
	if err := s.SetUserEmailVerified(ctx, alice.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	queueNotice(t, ctx, s, alice.ID, uuid.New(), "rebooted", "Hilltop", time.Now())
	pu, _ := url.Parse("https://beacon.example.org")
	m := &recMail{fail: errors.New("smtp: connection refused")}
	at := time.Now()
	sender := alerts.NewSender(AlertEval{s}, alerts.SenderConfig{Mail: m, PublicURL: pu, Now: func() time.Time { return at }})
	state := func() (pending, attempts int) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE delivered_at IS NULL), COALESCE(max(email_attempts), 0) FROM alert_outbox`).Scan(&pending, &attempts); err != nil {
			t.Fatal(err)
		}
		return
	}
	if err := sender.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if p, a := state(); p != 1 || a != 1 {
		t.Fatalf("after a failed send: %d pending, %d attempts", p, a)
	}
	// Still inside the delay: not tried again, even with the server back.
	m.fail = nil
	if err := sender.Run(ctx); err != nil || len(m.got) != 0 {
		t.Fatalf("retried inside the delay: %v %d", err, len(m.got))
	}
	at = at.Add(2 * time.Minute)
	if err := sender.Run(ctx); err != nil || len(m.got) != 1 {
		t.Fatalf("not sent after the delay: %v %d", err, len(m.got))
	}
	if p, _ := state(); p != 0 {
		t.Fatalf("%d still pending after the server took it", p)
	}
}

// A notice reaches every device of the user: the one that failed is retried alone, the one that got it is not
// sent it again.
func TestAlertPushPerDevicePostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	alice := newUser(t, ctx, s, "alice")
	phone, tablet := newSession(t, ctx, s, alice, "app", 1), newSession(t, ctx, s, alice, "app", 2)
	svc := alertSvc(s)
	for _, d := range []struct {
		id    uuid.UUID
		token string
	}{{phone.ID, "h_phone-token"}, {tablet.ID, "h_tablet-token"}} {
		if err := svc.RegisterDevice(ctx, alice.ID, d.id, "ios", d.token, testPayloadKey); err != nil {
			t.Fatal(err)
		}
	}
	queueNotice(t, ctx, s, alice.ID, uuid.New(), "rebooted", "Hilltop", time.Now())
	p := &flakyPush{fail: map[string]bool{"h_tablet-token": true}}
	at := time.Now()
	sender := alerts.NewSender(AlertEval{s}, alerts.SenderConfig{Push: p, PushID: "inst_test", Now: func() time.Time { return at }})
	if err := sender.Run(ctx); err != nil {
		t.Fatal(err)
	}
	var pending, reached int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM alert_outbox WHERE delivered_at IS NULL`).Scan(&pending)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM alert_outbox_devices WHERE session_id = $1`, phone.ID).Scan(&reached)
	if pending != 1 || reached != 1 || len(p.tokens) != 2 {
		t.Fatalf("pending %d, phone reached %d, sent to %v", pending, reached, p.tokens)
	}
	p.fail, p.tokens = nil, nil
	at = at.Add(2 * time.Minute)
	if err := sender.Run(ctx); err != nil {
		t.Fatal(err)
	}
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM alert_outbox WHERE delivered_at IS NULL`).Scan(&pending)
	if pending != 0 || len(p.tokens) != 1 || p.tokens[0] != "h_tablet-token" {
		t.Fatalf("pending %d, retry went to %v", pending, p.tokens)
	}
}

type flakyPush struct {
	fail   map[string]bool
	tokens []string
}

func (p *flakyPush) Send(_ context.Context, msgs []alerts.PushMessage) ([]alerts.PushResult, error) {
	out := make([]alerts.PushResult, len(msgs))
	for i, m := range msgs {
		p.tokens = append(p.tokens, m.Handle)
		if p.fail[m.Handle] {
			out[i].Status = alerts.PushRetry
		}
	}
	return out, nil
}

// All notices one user got in one pass go in one mail, even when the group would cross the end of a send batch.
func TestAlertEmailGroupsAreNotSplitPostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	var ids []uuid.UUID
	for _, name := range []string{"a1", "b2", "c3", "d4"} {
		u := newUser(t, ctx, s, name)
		if err := s.SetUserEmailVerified(ctx, u.ID, time.Now()); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, u.ID)
	}
	for _, id := range ids {
		pass := uuid.New()
		for range 80 {
			queueNotice(t, ctx, s, id, pass, "rebooted", "Hilltop", time.Now())
		}
	}
	pu, _ := url.Parse("https://beacon.example.org")
	m := &recMail{}
	sender := alerts.NewSender(AlertEval{s}, alerts.SenderConfig{Mail: m, PublicURL: pu})
	for range 2 {
		if err := sender.Run(ctx); err != nil {
			t.Fatal(err)
		}
	}
	perUser := map[string]int{}
	for _, got := range m.got {
		perUser[strings.SplitN(got, "|", 2)[0]]++
		if !strings.Contains(got, "|Beacon: 80 alerts|") {
			t.Fatalf("a message holds part of a pass: %.80q", got)
		}
	}
	if len(m.got) != 4 || len(perUser) != 4 {
		t.Fatalf("%d mails to %v", len(m.got), perUser)
	}
}

// advertWithDrift ingests an advert whose clock is drift seconds off, through the store as ingest does.
func advertWithDrift(t *testing.T, ctx context.Context, s *Store, key []byte, drift int64) {
	t.Helper()
	if _, err := s.UpsertNode(ctx, ingest.UpsertNodeParams{PublicKey: key, NodeType: 2, Name: "Hilltop", AdvertTimestamp: uint32(time.Now().Unix() + drift)}, ingest.RadioSettings{}); err != nil {
		t.Fatal(err)
	}
}

// Two consecutive adverts past the limit are needed: high, normal, high between two passes is not that.
func TestAlertClockNeedsConsecutiveAdvertsPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	svc := alertSvc(s)
	u := newUser(t, ctx, s, "alice")
	key := atlasKey(1)
	advertWithDrift(t, ctx, s, key, 0)
	pinAtlas(t, ctx, s, u, 0, api.StoredPin{Kind: "node", Key: key})
	if _, err := svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: key, Kind: "clock", Threshold: ipp(3600), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE user_alert_rules SET cursor_at = now() - interval '1 day'`); err != nil {
		t.Fatal(err)
	}
	task := alerts.NewTask(AlertEval{s}, time.Now)
	state := func() string {
		t.Helper()
		var st string
		if err := pool.QueryRow(ctx, `SELECT state FROM user_alert_rules`).Scan(&st); err != nil {
			t.Fatal(err)
		}
		return st
	}
	run := func(drifts ...int64) string {
		t.Helper()
		for _, d := range drifts {
			advertWithDrift(t, ctx, s, key, d)
			time.Sleep(5 * time.Millisecond)
		}
		if err := task.Run(ctx); err != nil {
			t.Fatal(err)
		}
		return state()
	}
	if got := run(7200); got == "firing" {
		t.Fatalf("one advert past the limit fired: %s", got)
	}
	if got := run(0, 7200); got == "firing" {
		t.Fatalf("high, normal, high fired: %s", got)
	}
	if got := run(7200); got != "firing" {
		t.Fatalf("two adverts in a row past the limit: %s", got)
	}
	if got := run(0); got != "ok" {
		t.Fatalf("a normal advert should recover: %s", got)
	}
}

func ruleStateOf(t *testing.T, ctx context.Context, pool interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, kind string) string {
	t.Helper()
	var st string
	if err := pool.QueryRow(ctx, `SELECT state FROM user_alert_rules WHERE kind = $1`, kind).Scan(&st); err != nil {
		t.Fatal(err)
	}
	return st
}

// Readings from days ago do not make a radio polled: it needs a polling assignment (a repeater) or fresh status
// telemetry (an observer). The overview's flag and the evaluation use the same answer.
func TestAlertPolledNeedsACurrentSourcePostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	svc := alertSvc(s)
	u := newUser(t, ctx, s, "alice")
	rep, obsKey := atlasKey(1), atlasKey(2)
	insertNode(t, ctx, s, rep, 2, "Hilltop")
	insertReading(t, ctx, s, rep, time.Now().Add(-47*time.Hour), 3000, nil)
	insertReading(t, ctx, s, rep, time.Now().Add(-48*time.Hour), 3000, nil)
	var obsID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO observers (public_key, display_name, last_seen, last_status_at) VALUES ($1, 'North', now(), now()) RETURNING id`, obsKey).Scan(&obsID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO observer_telemetry (id, observer_id, reported_at, battery_voltage_mv, noise_floor_db, uptime_seconds)
		VALUES (1, $1, now() - interval '30 hours', 3000, -100, 5000), (2, $1, now() - interval '31 hours', 3000, -100, 4000)`, obsID); err != nil {
		t.Fatal(err)
	}
	pinAtlas(t, ctx, s, u, 0, api.StoredPin{Kind: "node", Key: rep}, api.StoredPin{Kind: "observer", Key: obsKey})
	for _, r := range []struct {
		kind string
		key  []byte
		tk   string
	}{{"battery_low", rep, "node"}, {"noise_high", obsKey, "observer"}} {
		th := 3500
		if r.kind == "noise_high" {
			th = -130
		}
		if _, err := pool.Exec(ctx, `INSERT INTO user_alert_rules (user_id, target_kind, target_public_key, kind, threshold, cursor_at)
			VALUES ($1, $2, $3, $4, $5, now() - interval '3 days')`, u.ID, r.tk, r.key, r.kind, th); err != nil {
			t.Fatal(err)
		}
	}
	task := alerts.NewTask(AlertEval{s}, time.Now)
	polled := func() (node, observer bool) {
		t.Helper()
		ov, err := svc.Overview(ctx, u.ID)
		if err != nil || len(ov.Targets) != 2 {
			t.Fatalf("%+v %v", ov, err)
		}
		return ov.Targets[0].Caps.Polled, ov.Targets[1].Caps.Polled
	}
	pass := func() (string, string) {
		t.Helper()
		if err := task.Run(ctx); err != nil {
			t.Fatal(err)
		}
		return ruleStateOf(t, ctx, pool, "battery_low"), ruleStateOf(t, ctx, pool, "noise_high")
	}

	// Only history: not polled, and the rules read as inactive.
	if n, o := polled(); n || o {
		t.Fatalf("history alone made them polled: %v %v", n, o)
	}
	if a, b := pass(); a != "inactive" || b != "inactive" {
		t.Fatalf("rules without a source: %s %s", a, b)
	}
	// A polling assignment and a fresh telemetry row bring both back.
	assignPoller(t, ctx, s, rep)
	if _, err := pool.Exec(ctx, `INSERT INTO observer_telemetry (id, observer_id, reported_at, battery_voltage_mv, noise_floor_db, uptime_seconds)
		VALUES (3, $1, now() - interval '5 minutes', 3000, -60, 6000), (4, $1, now() - interval '10 minutes', 3000, -60, 5900)`, obsID); err != nil {
		t.Fatal(err)
	}
	if n, o := polled(); !n || !o {
		t.Fatalf("a current source is not polled: %v %v", n, o)
	}
	if a, b := pass(); a != "firing" || b != "firing" {
		t.Fatalf("rules with a source: %s %s", a, b)
	}
	// Taking the assignment away makes the repeater's rule inactive again.
	if _, err := pool.Exec(ctx, `DELETE FROM polling_assignments`); err != nil {
		t.Fatal(err)
	}
	if n, _ := polled(); n {
		t.Fatal("still polled without an assignment")
	}
	if a, _ := pass(); a != "inactive" {
		t.Fatalf("rule after the source went: %s", a)
	}
}

// The pass cursor is kept between passes and a pass that reached the end of the rules starts over next time.
func TestAlertEvaluationCursorPostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	a := AlertEval{s}
	if id, err := a.LoadCursor(ctx); err != nil || id != uuid.Nil {
		t.Fatalf("fresh cursor: %v %v", id, err)
	}
	want := uuid.New()
	if err := a.SaveCursor(ctx, want); err != nil {
		t.Fatal(err)
	}
	if id, err := a.LoadCursor(ctx); err != nil || id != want {
		t.Fatalf("saved cursor: %v %v", id, err)
	}
	// Nothing to evaluate: the pass is at the end at once, so the cursor goes back to the start.
	if err := alerts.NewTask(a, time.Now).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if id, err := a.LoadCursor(ctx); err != nil || id != uuid.Nil {
		t.Fatalf("cursor after a pass that reached the end: %v %v", id, err)
	}
}

type holdPush struct {
	calls int
	wait  time.Duration
}

func (p *holdPush) Send(_ context.Context, msgs []alerts.PushMessage) ([]alerts.PushResult, error) {
	p.calls++
	out := make([]alerts.PushResult, len(msgs))
	if p.wait > 0 {
		for i := range out {
			out[i] = alerts.PushResult{Status: alerts.PushRetry, RetryAfter: p.wait}
		}
	}
	return out, nil
}

// One pass sends the mail and the push; a rate-limited push holds the row back as long as asked, and its retry
// never sends the mail again.
func TestAlertChannelsRetryOnlyWhatFailedPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	alice := newUser(t, ctx, s, "alice")
	if err := s.SetUserEmailVerified(ctx, alice.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	sess := newSession(t, ctx, s, alice, "app", 1)
	if err := alertSvc(s).RegisterDevice(ctx, alice.ID, sess.ID, "ios", "h_phone", testPayloadKey); err != nil {
		t.Fatal(err)
	}
	queueNotice(t, ctx, s, alice.ID, uuid.New(), "rebooted", "Hilltop", time.Now())
	pu, _ := url.Parse("https://beacon.example.org")
	m, p := &recMail{}, &holdPush{wait: 30 * time.Minute}
	at := time.Now()
	sender := alerts.NewSender(AlertEval{s}, alerts.SenderConfig{Mail: m, Push: p, PushID: "inst_test", PublicURL: pu, Now: func() time.Time { return at }})
	if err := sender.Run(ctx); err != nil {
		t.Fatal(err)
	}
	var sent, delivered bool
	var next time.Time
	if err := pool.QueryRow(ctx, `SELECT email_sent_at IS NOT NULL, delivered_at IS NOT NULL, push_next_attempt_at FROM alert_outbox`).Scan(&sent, &delivered, &next); err != nil {
		t.Fatal(err)
	}
	if len(m.got) != 1 || p.calls != 1 || !sent || delivered || next.Before(at.Add(29*time.Minute)) {
		t.Fatalf("mails %d pushes %d sent %v delivered %v next %v", len(m.got), p.calls, sent, delivered, next.Sub(at))
	}
	// The usual one-minute delay is not enough: the push server asked for half an hour.
	at = at.Add(5 * time.Minute)
	if err := sender.Run(ctx); err != nil || p.calls != 1 {
		t.Fatalf("retried inside the hold: %v %d", err, p.calls)
	}
	at = at.Add(30 * time.Minute)
	p.wait = 0
	if err := sender.Run(ctx); err != nil {
		t.Fatal(err)
	}
	_ = pool.QueryRow(ctx, `SELECT delivered_at IS NOT NULL FROM alert_outbox`).Scan(&delivered)
	if len(m.got) != 1 || p.calls != 2 || !delivered {
		t.Fatalf("mails %d pushes %d delivered %v", len(m.got), p.calls, delivered)
	}
}

// A user who turned both channels off has the notice finished without sending anything.
func TestAlertChannelsOffFinishTheRowPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	alice := newUser(t, ctx, s, "alice")
	if err := s.SetUserEmailVerified(ctx, alice.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	off := false
	if _, err := alertSvc(s).SetChannels(ctx, alice.ID, &off, &off); err != nil {
		t.Fatal(err)
	}
	queueNotice(t, ctx, s, alice.ID, uuid.New(), "rebooted", "Hilltop", time.Now())
	m, p := &recMail{}, &holdPush{}
	if err := alerts.NewSender(AlertEval{s}, alerts.SenderConfig{Mail: m, Push: p, PushID: "inst_test"}).Run(ctx); err != nil {
		t.Fatal(err)
	}
	var pending int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM alert_outbox WHERE delivered_at IS NULL`).Scan(&pending)
	if len(m.got) != 0 || p.calls != 0 || pending != 0 {
		t.Fatalf("mails %d pushes %d pending %d", len(m.got), p.calls, pending)
	}
}
