// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
	"github.com/google/uuid"
)

// mailAndPushUsers makes n users with a verified address, one app device and one queued notice each.
func mailAndPushUsers(t *testing.T, ctx context.Context, s *Store, n int) {
	t.Helper()
	svc := alertSvc(s)
	for i := range n {
		u := newUser(t, ctx, s, fmt.Sprintf("user%03d", i))
		if err := s.SetUserEmailVerified(ctx, u.ID, time.Now()); err != nil {
			t.Fatal(err)
		}
		sess := newSession(t, ctx, s, u, "app", byte(i))
		if err := svc.RegisterDevice(ctx, u.ID, sess.ID, "ios", fmt.Sprintf("h_user%03d", i), testPayloadKey); err != nil {
			t.Fatal(err)
		}
		queueNotice(t, ctx, s, u.ID, uuid.New(), "rebooted", "Hilltop", time.Now())
	}
}

func mailedUsers(m *recMail) int {
	to := map[string]bool{}
	for _, got := range m.got {
		to[strings.SplitN(got, "|", 2)[0]] = true
	}
	return len(to)
}

// A push server that fails every call uses up the push attempts, but the mail capped in earlier runs is still sent:
// the notice is dropped only once both channels are done.
func TestAlertPushFailureNeverDropsUnsentMailPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	n := alerts.MaxMailsPerPass*alerts.MaxAttempts + 4
	mailAndPushUsers(t, ctx, s, n)
	pu, _ := url.Parse("https://beacon.example.org")
	m, p := &recMail{}, &failPush{}
	at := time.Now()
	sender := alerts.NewSender(AlertEval{s}, alerts.SenderConfig{Mail: m, Push: p, PushID: "inst_test", PublicURL: pu, Now: func() time.Time { return at }})
	for range alerts.MaxAttempts + 1 {
		if err := sender.Run(ctx); err != nil {
			t.Fatal(err)
		}
		at = at.Add(10 * time.Minute)
	}
	var left int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM alert_outbox`).Scan(&left)
	if got := mailedUsers(m); got != n || len(m.got) != n || left != 0 {
		t.Fatalf("mailed %d of %d users (%d mails), %d rows left", got, n, len(m.got), left)
	}
}

// A push server asking to wait for hours holds back the push only: the mails capped in the first run go out in the
// next one.
func TestAlertPushQuotaHoldNeverHoldsMailPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	n := alerts.MaxMailsPerPass + 4
	mailAndPushUsers(t, ctx, s, n)
	pu, _ := url.Parse("https://beacon.example.org")
	m, p := &recMail{}, &quotaPush{wait: 6 * time.Hour}
	at := time.Now()
	sender := alerts.NewSender(AlertEval{s}, alerts.SenderConfig{Mail: m, Push: p, PushID: "inst_test", PublicURL: pu, Now: func() time.Time { return at }})
	for range 2 {
		if err := sender.Run(ctx); err != nil {
			t.Fatal(err)
		}
		at = at.Add(time.Minute)
	}
	var pushHeld, emailDone, attempts int
	_ = pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE push_next_attempt_at > $1), count(*) FILTER (WHERE email_done_at IS NOT NULL),
		COALESCE(max(push_attempts), 0) FROM alert_outbox WHERE delivered_at IS NULL`, at.Add(5*time.Hour)).Scan(&pushHeld, &emailDone, &attempts)
	if mailedUsers(m) != n || pushHeld != n || emailDone != n || p.calls != 1 || attempts != 0 {
		t.Fatalf("mailed %d of %d, push held %d, email done %d, push calls %d, push attempts %d", mailedUsers(m), n, pushHeld, emailDone, p.calls, attempts)
	}
}

type quotaPush struct {
	calls int
	wait  time.Duration
}

func (p *quotaPush) Send(context.Context, []alerts.PushMessage) ([]alerts.PushResult, error) {
	p.calls++
	return nil, &alerts.PushError{Status: 429, Code: "rate_limited", RetryAfter: p.wait}
}

// slowRelay answers /v1/send like the push server, handing messages on one at a time; each takes perMessage, and
// the work goes on after the caller gave up, as it does there.
type slowRelay struct {
	perMessage time.Duration
	mu         sync.Mutex
	handed     map[string]int // collapse key: times handed on
}

func (r *slowRelay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var body struct {
		Messages []struct {
			CollapseKey string `json:"collapseKey"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		http.Error(w, "bad", http.StatusBadRequest)
		return
	}
	results := make([]map[string]string, len(body.Messages))
	for i, m := range body.Messages {
		time.Sleep(r.perMessage)
		r.mu.Lock()
		r.handed[m.CollapseKey]++
		r.mu.Unlock()
		results[i] = map[string]string{"status": "ok"}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
}

// A full batch takes longer than the call may last, so the call times out after part of it went out. The timed-out
// notices go again in smaller batches, and the others are not held back meanwhile: every notice is delivered
// without running out of attempts.
func TestAlertPushSlowRelayDeliversEverythingPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	alice := newUser(t, ctx, s, "alice")
	sess := newSession(t, ctx, s, alice, "app", 1)
	if err := alertSvc(s).RegisterDevice(ctx, alice.ID, sess.ID, "ios", "h_phone", testPayloadKey); err != nil {
		t.Fatal(err)
	}
	const notices = 3 * alerts.MaxPushBatch
	pass := uuid.New()
	for range notices {
		queueNotice(t, ctx, s, alice.ID, pass, "rebooted", "Hilltop", time.Now())
	}
	relay := &slowRelay{perMessage: 2 * time.Millisecond, handed: map[string]int{}}
	srv := httptest.NewServer(relay)
	defer srv.Close()
	// A full batch needs over 200 ms, half of one about 100 ms.
	client := &http.Client{Timeout: 170 * time.Millisecond}
	at := time.Now()
	sender := alerts.NewSender(AlertEval{s}, alerts.SenderConfig{Push: alerts.NewRelay(srv.URL, "inst_test", "secret", client),
		PushID: "inst_test", Now: func() time.Time { return at }})
	for range alerts.MaxAttempts {
		if err := sender.Run(ctx); err != nil {
			t.Fatal(err)
		}
		at = at.Add(10 * time.Minute)
	}
	var total, delivered, maxAttempts int
	_ = pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE delivered_at IS NOT NULL), COALESCE(max(push_attempts), 0) FROM alert_outbox`).
		Scan(&total, &delivered, &maxAttempts)
	relay.mu.Lock()
	handed := len(relay.handed)
	relay.mu.Unlock()
	if total != notices || delivered != notices || handed != notices || maxAttempts > 1 {
		t.Fatalf("rows %d, delivered %d, handed on %d of %d, most push attempts %d", total, delivered, handed, notices, maxAttempts)
	}
}

// The evaluator stores a number for every notice; the push for a kind whose sentence has no figure still carries
// null.
func TestAlertValuelessNoticesSendNullPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	alice := newUser(t, ctx, s, "alice")
	sess := newSession(t, ctx, s, alice, "app", 1)
	if err := alertSvc(s).RegisterDevice(ctx, alice.ID, sess.ID, "ios", "h_phone", testPayloadKey); err != nil {
		t.Fatal(err)
	}
	rep := atlasKey(1)
	insertNode(t, ctx, s, rep, 2, "Hilltop")
	if _, err := pool.Exec(ctx, `INSERT INTO telemetry_requests (id, user_id, target_public_key, status, created_at, updated_at, enabled_at)
		VALUES (gen_random_uuid(), $1, $2, 'login_failed', now(), now(), now())`, alice.ID, rep); err != nil {
		t.Fatal(err)
	}
	if err := alerts.NewTask(AlertEval{s}, time.Now).Run(ctx); err != nil {
		t.Fatal(err)
	}
	queueNotice(t, ctx, s, alice.ID, uuid.New(), "rebooted", "Hilltop", time.Now())
	queueNotice(t, ctx, s, alice.ID, uuid.New(), "back_online", "Hilltop", time.Now())
	queueNotice(t, ctx, s, alice.ID, uuid.New(), "gone_quiet", "Hilltop", time.Now())
	p := &recPush{}
	if err := alerts.NewSender(AlertEval{s}, alerts.SenderConfig{Push: p, PushID: "inst_test"}).Run(ctx); err != nil {
		t.Fatal(err)
	}
	got := map[string]any{}
	for _, m := range p.sent {
		n := openNotice(t, m.Ciphertext)
		got[n["kind"].(string)] = n["value"]
	}
	if len(got) != 4 || got["request_attention"] != nil || got["rebooted"] != nil || got["back_online"] != nil || got["gone_quiet"] != float64(30) {
		t.Fatalf("values %v", got)
	}
}

// rateRelay answers /v1/send like a push server over its quota: every item rate_limited with its own wait, or the
// whole call 429 with a Retry-After header.
type rateRelay struct {
	itemWait  int // seconds; 0 answers 429 instead
	callAfter string
	calls     int
}

func (r *rateRelay) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.calls++
	var body struct {
		Messages []json.RawMessage `json:"messages"`
	}
	_ = json.NewDecoder(req.Body).Decode(&body)
	if r.itemWait == 0 {
		w.Header().Set("Retry-After", r.callAfter)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"code":"rate_limited","message":"daily quota"}}`))
		return
	}
	results := make([]map[string]any, len(body.Messages))
	for i := range results {
		results[i] = map[string]any{"status": "rate_limited", "retryAfterSeconds": r.itemWait}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
}

// A rate-limited push, per item or for the whole call, waits as long as the push server asked and never counts an
// attempt: run after run it is held, not used up, while the mail goes out at once. Only MaxAge ends it.
func TestAlertPushRateLimitHoldsWithoutAttemptsPostgres(t *testing.T) {
	for _, tc := range []struct {
		name  string
		relay *rateRelay
		wait  time.Duration
	}{
		{"per item", &rateRelay{itemWait: 900}, 15 * time.Minute},
		{"whole call", &rateRelay{callAfter: "600"}, 10 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, s := userStore(t)
			mailAndPushUsers(t, ctx, s, 3)
			srv := httptest.NewServer(tc.relay)
			defer srv.Close()
			pu, _ := url.Parse("https://beacon.example.org")
			m := &recMail{}
			at := time.Now()
			sender := alerts.NewSender(AlertEval{s}, alerts.SenderConfig{Mail: m, Push: alerts.NewRelay(srv.URL, "inst_test", "secret", nil),
				PushID: "inst_test", PublicURL: pu, Now: func() time.Time { return at }})
			state := func() (pending, attempts int, next time.Time) {
				t.Helper()
				if err := pool.QueryRow(ctx, `SELECT count(*), COALESCE(max(push_attempts), 0), COALESCE(min(push_next_attempt_at), 'epoch')
					FROM alert_outbox WHERE delivered_at IS NULL`).Scan(&pending, &attempts, &next); err != nil {
					t.Fatal(err)
				}
				return
			}
			if err := sender.Run(ctx); err != nil {
				t.Fatal(err)
			}
			pending, attempts, next := state()
			if len(m.got) != 3 || pending != 3 || attempts != 0 || next.Sub(at) < tc.wait-time.Second || next.Sub(at) > tc.wait+time.Second {
				t.Fatalf("mails %d pending %d attempts %d held %v", len(m.got), pending, attempts, next.Sub(at))
			}
			// Inside the hold nothing is tried; past it, again and again, more often than MaxAttempts, it is held anew.
			at = at.Add(time.Minute)
			if err := sender.Run(ctx); err != nil || tc.relay.calls != 1 {
				t.Fatalf("tried inside the hold: %v %d", err, tc.relay.calls)
			}
			for range alerts.MaxAttempts + 1 {
				at = at.Add(tc.wait)
				if err := sender.Run(ctx); err != nil {
					t.Fatal(err)
				}
			}
			pending, attempts, _ = state()
			if pending != 3 || attempts != 0 || tc.relay.calls != alerts.MaxAttempts+2 || len(m.got) != 3 {
				t.Fatalf("pending %d attempts %d calls %d mails %d", pending, attempts, tc.relay.calls, len(m.got))
			}
			// MaxAge still drops it.
			at = at.Add(alerts.MaxAge)
			if err := sender.Run(ctx); err != nil {
				t.Fatal(err)
			}
			var left int
			_ = pool.QueryRow(ctx, `SELECT count(*) FROM alert_outbox`).Scan(&left)
			if left != 0 {
				t.Fatalf("%d rows past MaxAge", left)
			}
		})
	}
}

// Upgrading from 084: the shared attempts and hold came from push, so unsent mail starts fresh and due now, sent
// mail stays done, and push keeps its count and hold.
func TestAlertOutboxChannelMigrationPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	// Back to the 084 shape, so 085 runs again on it.
	if _, err := pool.Exec(ctx, `DELETE FROM schema_migrations WHERE filename = '085_alert_outbox_channel_retries.sql';
		ALTER TABLE alert_outbox DROP COLUMN email_attempts, DROP COLUMN email_next_attempt_at, DROP COLUMN email_done_at,
			DROP COLUMN push_attempts, DROP COLUMN push_next_attempt_at, DROP COLUMN push_done_at,
			ADD COLUMN attempts smallint NOT NULL DEFAULT 0, ADD COLUMN next_attempt_at timestamp with time zone`); err != nil {
		t.Fatal(err)
	}
	alice := newUser(t, ctx, s, "alice")
	now := time.Now()
	seed := func(attempts int, next time.Time, emailSent, delivered *time.Time) int64 {
		t.Helper()
		var id int64
		if err := pool.QueryRow(ctx, `INSERT INTO alert_outbox (user_id, pass_id, target_kind, target_public_key, alert_kind, value,
			attempts, next_attempt_at, email_sent_at, delivered_at) VALUES ($1, $2, 'node', $3, 'rebooted', 0, $4, $5, $6, $7) RETURNING id`,
			alice.ID, uuid.New(), atlasKey(1), attempts, next, emailSent, delivered).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	unmailed := seed(4, now.Add(6*time.Hour), nil, nil)
	mailed := seed(2, now.Add(time.Hour), &now, nil)
	done := seed(0, time.Time{}, &now, &now)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	type st struct {
		emailAttempts, pushAttempts int
		emailNext, pushNext         *time.Time
		emailDone, pushDone         bool
	}
	read := func(id int64) st {
		t.Helper()
		var r st
		if err := pool.QueryRow(ctx, `SELECT email_attempts, push_attempts, email_next_attempt_at, push_next_attempt_at,
			email_done_at IS NOT NULL, push_done_at IS NOT NULL FROM alert_outbox WHERE id = $1`, id).
			Scan(&r.emailAttempts, &r.pushAttempts, &r.emailNext, &r.pushNext, &r.emailDone, &r.pushDone); err != nil {
			t.Fatal(err)
		}
		return r
	}
	u := read(unmailed)
	if u.emailAttempts != 0 || (u.emailNext != nil && u.emailNext.After(time.Now())) || u.emailDone ||
		u.pushAttempts != 4 || u.pushNext == nil || u.pushNext.Sub(now) < 5*time.Hour || u.pushDone {
		t.Fatalf("unmailed: %+v", u)
	}
	if m := read(mailed); !m.emailDone || m.pushAttempts != 2 || m.pushDone {
		t.Fatalf("mailed: %+v", m)
	}
	if d := read(done); !d.emailDone || !d.pushDone {
		t.Fatalf("delivered: %+v", d)
	}
}
