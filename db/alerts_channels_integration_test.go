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
	var pushHeld, emailDone int
	_ = pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE push_next_attempt_at > $1), count(*) FILTER (WHERE email_done_at IS NOT NULL)
		FROM alert_outbox WHERE delivered_at IS NULL`, at.Add(5*time.Hour)).Scan(&pushHeld, &emailDone)
	if mailedUsers(m) != n || pushHeld != n || emailDone != n || p.calls != 1 {
		t.Fatalf("mailed %d of %d, push held %d, email done %d, push calls %d", mailedUsers(m), n, pushHeld, emailDone, p.calls)
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
	relay := &slowRelay{perMessage: 20 * time.Millisecond, handed: map[string]int{}}
	srv := httptest.NewServer(relay)
	defer srv.Close()
	// A full batch needs 400 ms, half of one 200 ms.
	client := &http.Client{Timeout: 300 * time.Millisecond}
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
