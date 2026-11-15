// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/google/uuid"
)

// claimStore adds the passwords in use to the in-memory request store; withPasswords puts them on owner rows the way
// the store's owner read does.
type claimStore struct {
	*memRequests
	passwrds map[string]RequestedPassword
}

func newClaimStore() *claimStore {
	return &claimStore{memRequests: newMem(), passwrds: map[string]RequestedPassword{}}
}

func (s *claimStore) withPasswords(rows []OwnerRow) []OwnerRow {
	for i := range rows {
		if p, ok := s.passwrds[string(rows[i].Target)]; ok && rows[i].Requested {
			rows[i].Password = &p
		}
	}
	return rows
}

func (s *claimStore) RecordServedVersions(_ context.Context, observer []byte, vers map[string]int64, _ time.Time) error {
	for t, v := range vers {
		st := s.states[t]
		if st == nil {
			continue
		}
		if st.ServedVer == nil {
			st.ServedVer = map[string]int64{}
		}
		st.ServedVer[string(observer)] = v
	}
	return nil
}

// ReplacePollingClaimReporting runs the reports the way the store does, one target state at a time.
func (s *claimStore) ReplacePollingClaimReporting(ctx context.Context, c Claim, _ uuid.UUID, _ time.Time, report func(ClaimNode, *TargetState)) (bool, error) {
	for _, n := range c.Nodes {
		if !n.Requested {
			continue
		}
		if err := s.MutateRequestTarget(ctx, n.Target, uuid.Nil, func(st *TargetState) error { report(n, st); return nil }); err != nil {
			return false, err
		}
	}
	return true, nil
}

// reportClaim applies a claim's reports as HandleClaim does.
func reportClaim(ctx context.Context, svc *Service, c Claim, now time.Time) {
	var ignored [][]byte
	if _, err := svc.reqs.ReplacePollingClaimReporting(ctx, c, uuid.Nil, now, claimReport(c, now, WorkingPollFresh(svc.cfg), &ignored)); err != nil {
		panic(err)
	}
}

// hold records that the observer was handed the target as a requested one, at the rank.
func (s *claimStore) hold(target, observer []byte, rank int) {
	st := s.states[string(target)]
	if st.Holders == nil {
		st.Holders = map[string]int{}
	}
	st.Holders[string(observer)] = rank
}

var claimNow = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

func claimService(t *testing.T) (*Service, *claimStore, *Sealer) {
	t.Helper()
	st := newClaimStore()
	sealer, err := NewSealer(bytes.Repeat([]byte{4}, 32))
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(newFakeStore(), config.ResolvedPolling{Enabled: true}, func() time.Time { return claimNow }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	svc.SetRequests(st, sealer)
	return svc, st, sealer
}

// requestedTarget stores one request (with a password when pw is not empty) and returns its target.
func requestedTarget(t *testing.T, st *claimStore, sealer *Sealer, key byte, pw string, age int) (target []byte, id uuid.UUID) {
	t.Helper()
	target = bytes.Repeat([]byte{key}, 32)
	r := Request{ID: uuid.New(), UserID: uuid.New(), CreatedAt: claimNow.Add(time.Duration(age) * time.Minute), EnabledAt: claimNow}
	if pw != "" {
		var err error
		if r.PasswordCT, r.PasswordNonce, err = sealer.Seal(r.ID, target, []byte(pw)); err != nil {
			t.Fatal(err)
		}
		r.HasPassword = true
	}
	err := st.MutateRequestTarget(context.Background(), target, r.UserID, func(ts *TargetState) error { return ts.Create(r, claimNow) })
	if err != nil {
		t.Fatal(err)
	}
	return target, r.ID
}

func claimOf(observer []byte, nodes ...ClaimNode) Claim {
	return Claim{Observer: observer, Timestamp: claimNow, AcceptsRequests: true, RequestSlots: 3, Nodes: nodes}
}

func statusOf(st *claimStore, target []byte, i int) RequestStatus {
	return st.states[string(target)].Requests[i].Status
}

func TestClaimLoginFailedOnlyCountsFromThePoller(t *testing.T) {
	ctx := context.Background()
	svc, st, sealer := claimService(t)
	target, _ := requestedTarget(t, st, sealer, 1, "guest", 0)
	poller, standby := bytes.Repeat([]byte{0xA}, 32), bytes.Repeat([]byte{0xB}, 32)
	failed := ClaimNode{Target: target, Requested: true, State: "login_failed"}
	st.hold(target, poller, 0)
	st.hold(target, standby, 1)

	// An hour after the password was set, so the grace is over.
	hour := claimNow.Add(time.Hour)
	reportClaim(ctx, svc, claimOf(standby, failed), hour)
	if got := statusOf(st, target, 0); got == StatusLoginFailed {
		t.Fatal("a standby's login_failed counted")
	}
	reportClaim(ctx, svc, claimOf(poller, failed), hour)
	if got := statusOf(st, target, 0); got != StatusLoginFailed {
		t.Fatalf("the poller's login_failed was ignored: %s", got)
	}
}

func TestClaimLoginFailedGraceAfterAVerChange(t *testing.T) {
	ctx := context.Background()
	svc, st, sealer := claimService(t)
	target, _ := requestedTarget(t, st, sealer, 1, "guest", 0)
	// The two later requests give login_failed somewhere to go after the first one fails.
	for i := 1; i <= 2; i++ {
		requestedTarget2(t, st, sealer, target, "pw"+string(rune('0'+i)), i)
	}
	poller := bytes.Repeat([]byte{0xA}, 32)
	st.hold(target, poller, 0)
	failed := ClaimNode{Target: target, Requested: true, State: "login_failed"}
	ok := ClaimNode{Target: target, Requested: true, State: "ok"}

	hour := claimNow.Add(time.Hour)
	reportClaim(ctx, svc, claimOf(poller, failed), hour)
	if got := st.states[string(target)]; got.Requests[0].Status != StatusLoginFailed || got.Ver != 2 {
		t.Fatalf("first failure: status %s ver %d", got.Requests[0].Status, got.Ver)
	}
	// The device has not fetched the new ver yet: its repeated login_failed is ignored for 30 minutes...
	later := hour.Add(5 * time.Minute)
	reportClaim(ctx, svc, claimOf(poller, failed), later)
	if statusOf(st, target, 1) == StatusLoginFailed {
		t.Fatal("login_failed inside the grace counted")
	}
	// ...unless a claim showed another state since the change.
	reportClaim(ctx, svc, claimOf(poller, ok), later.Add(time.Minute))
	reportClaim(ctx, svc, claimOf(poller, failed), later.Add(2*time.Minute))
	if statusOf(st, target, 1) != StatusLoginFailed {
		t.Fatal("login_failed after another state was ignored")
	}
}

// requestedTarget2 adds a request for an existing target.
func requestedTarget2(t *testing.T, st *claimStore, sealer *Sealer, target []byte, pw string, age int) {
	t.Helper()
	r := Request{ID: uuid.New(), UserID: uuid.New(), CreatedAt: claimNow.Add(time.Duration(age) * time.Minute), EnabledAt: claimNow}
	var err error
	if r.PasswordCT, r.PasswordNonce, err = sealer.Seal(r.ID, target, []byte(pw)); err != nil {
		t.Fatal(err)
	}
	r.HasPassword = true
	if err := st.MutateRequestTarget(context.Background(), target, r.UserID, func(ts *TargetState) error { return ts.Create(r, claimNow) }); err != nil {
		t.Fatal(err)
	}
}

func TestClaimAdminStopDeletesThePasswordOnce(t *testing.T) {
	ctx := context.Background()
	svc, st, sealer := claimService(t)
	target, _ := requestedTarget(t, st, sealer, 1, "admin-pw", 0)
	requestedTarget2(t, st, sealer, target, "guest", 1)
	a, b := bytes.Repeat([]byte{0xA}, 32), bytes.Repeat([]byte{0xB}, 32)
	stop := ClaimNode{Target: target, Requested: true, State: "admin_stop"}
	st.hold(target, b, 1)
	st.hold(target, a, 2)

	// Any observer's report counts, a standby's too.
	reportClaim(ctx, svc, claimOf(b, stop), claimNow)
	r := st.states[string(target)].Requests[0]
	if r.Status != StatusAdminStopped || r.HasPassword || r.PasswordCT != nil {
		t.Fatalf("admin password kept: %+v", r)
	}
	if !st.states[string(target)].AdminStops[string(b)] {
		t.Fatal("stop not recorded")
	}
	// The same observer repeating its report must not strike the next user's password.
	reportClaim(ctx, svc, claimOf(b, stop), claimNow.Add(time.Minute))
	if got := statusOf(st, target, 1); got == StatusAdminStopped {
		t.Fatal("a repeat report stopped the next password")
	}
	// Another observer that also got admin on it does.
	reportClaim(ctx, svc, claimOf(a, stop), claimNow.Add(2*time.Minute))
	if got := statusOf(st, target, 1); got != StatusAdminStopped {
		t.Fatalf("second observer's report: %s", got)
	}
}

func TestClaimAdminStopWithBlankLoginBlocksThem(t *testing.T) {
	ctx := context.Background()
	svc, st, sealer := claimService(t)
	target, _ := requestedTarget(t, st, sealer, 1, "", 0)
	st.hold(target, bytes.Repeat([]byte{0xA}, 32), 0)
	reportClaim(ctx, svc, claimOf(bytes.Repeat([]byte{0xA}, 32), ClaimNode{Target: target, Requested: true, State: "admin_stop"}), claimNow)
	s := st.states[string(target)]
	if !s.NoPasswordBlocked || s.Requests[0].Status != StatusAdminStopped {
		t.Fatalf("%+v", s)
	}
}

func TestClaimIgnoresTargetsItDoesNotHoldAsRequested(t *testing.T) {
	ctx := context.Background()
	svc, st, sealer := claimService(t)
	target, _ := requestedTarget(t, st, sealer, 1, "guest", 0)
	o := bytes.Repeat([]byte{0xA}, 32)
	st.hold(target, o, 0)
	// A local target of the same key (requested not set) must not touch the requests.
	reportClaim(ctx, svc, claimOf(o, ClaimNode{Target: target, State: "login_failed"}, ClaimNode{Target: target, State: "admin_stop"}), claimNow)
	if got := statusOf(st, target, 0); got != StatusWaiting || len(st.states[string(target)].AdminStops) != 0 {
		t.Fatalf("status %s", got)
	}
}

// device is an observer with a real key pair.
type device struct {
	pub []byte
	prv []byte // expanded, scalar first
}

func newDevice(t *testing.T) device {
	t.Helper()
	seed := make([]byte, 32)
	if _, err := rand.Read(seed); err != nil {
		t.Fatal(err)
	}
	return device{pub: ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey), prv: expandSeed(seed)}
}

func ownersFor(t *testing.T, svc *Service, d device, rows []OwnerRow) map[string]OwnerEntry {
	t.Helper()
	if st, ok := svc.reqs.(*claimStore); ok {
		rows = st.withPasswords(rows)
	}
	nodes := BuildOwners(rows, DisabledPolicy()).Nodes
	if err := svc.addEnvelopes(context.Background(), d.pub, rows, nodes); err != nil {
		t.Fatal(err)
	}
	return nodes
}

func TestOwnersEnvelopeOpensToThePassword(t *testing.T) {
	svc, st, sealer := claimService(t)
	d := newDevice(t)
	target, id := requestedTarget(t, st, sealer, 7, "guest-pw", 0)
	ct, nonce, _ := sealer.Seal(id, target, []byte("guest-pw"))
	st.passwrds[string(target)] = RequestedPassword{RequestID: id, Ver: 5, CT: ct, Nonce: nonce}
	rows := []OwnerRow{{Target: target, Rank: 0, Requested: true, PWOK: true}}

	e := ownersFor(t, svc, d, rows)[hex.EncodeToString(target)]
	if !e.Requested || e.PW == nil || e.PW.Ver != 5 || e.Role != "polling" || !e.Mine {
		t.Fatalf("%+v", e)
	}
	pw, err := OpenEnvelope(d.prv, d.pub, target, *e.PW)
	if err != nil || string(pw) != "guest-pw" {
		t.Fatalf("opened %q %v", pw, err)
	}
	// A fresh envelope on every answer, same version.
	e2 := ownersFor(t, svc, d, rows)[hex.EncodeToString(target)]
	if e2.PW.EPK == e.PW.EPK || e2.PW.Ver != e.PW.Ver {
		t.Fatal("envelope reused")
	}
	// Another observer cannot open it.
	if _, err := OpenEnvelope(newDevice(t).prv, d.pub, target, *e.PW); err == nil {
		t.Fatal("opened with another key")
	}
	b, _ := json.Marshal(e)
	if !strings.Contains(string(b), `"requested":true`) || !strings.Contains(string(b), `"pw":{"epk"`) || strings.Contains(string(b), "guest-pw") {
		t.Fatalf("%s", b)
	}
}

func TestOwnersRequestedEntryRules(t *testing.T) {
	svc, st, sealer := claimService(t)
	d := newDevice(t)
	pwTarget, id := requestedTarget(t, st, sealer, 1, "guest", 0)
	ct, nonce, _ := sealer.Seal(id, pwTarget, []byte("guest"))
	st.passwrds[string(pwTarget)] = RequestedPassword{RequestID: id, Ver: 1, CT: ct, Nonce: nonce}
	blank := bytes.Repeat([]byte{2}, 32)
	st.passwrds[string(blank)] = RequestedPassword{Ver: 1} // the blank or hello option in use
	gone := bytes.Repeat([]byte{3}, 32)                    // no longer requested
	noSeat := bytes.Repeat([]byte{4}, 32)
	stale := bytes.Repeat([]byte{5}, 32)
	local := bytes.Repeat([]byte{6}, 32)
	rows := []OwnerRow{
		{Target: pwTarget, Rank: 1, Requested: true, PWOK: true},
		{Target: blank, Rank: 0, Requested: true, PWOK: true},
		{Target: gone, Rank: 0, Requested: true, PWOK: true},
		{Target: noSeat, Rank: 0, Requested: true, PWOK: false}, // claim stopped accepting requests
		{Target: stale, Rank: -1, ClaimedRequested: true},       // claims it as requested, not assigned
		{Target: local, Rank: 0},                                // an ordinary local target
	}
	n := ownersFor(t, svc, d, rows)
	key := func(b []byte) string { return hex.EncodeToString(b) }
	if e := n[key(pwTarget)]; e.Role != "standby" || !e.Requested || e.PW == nil {
		t.Errorf("standby with a password: %+v", e)
	}
	if e := n[key(blank)]; !e.Requested || e.PW != nil {
		t.Errorf("blank option has no pw: %+v", e)
	}
	for name, k := range map[string][]byte{"no longer requested": gone, "no seat": noSeat, "stale claim": stale} {
		if _, ok := n[key(k)]; ok {
			t.Errorf("%s: entry must be left out so the device drops it", name)
		}
	}
	if e := n[key(local)]; e.Requested || e.PW != nil || e.Role != "polling" {
		t.Errorf("local target touched: %+v", e)
	}
}

func TestOwnersLeavesOutTargetsWhoseEnvelopeFails(t *testing.T) {
	svc, st, sealer := claimService(t)
	d := newDevice(t)
	target, id := requestedTarget(t, st, sealer, 1, "guest", 0)
	ct, nonce, _ := sealer.Seal(id, target, []byte("guest"))
	rows := []OwnerRow{{Target: target, Rank: 0, Requested: true, PWOK: true}}

	// Sealed for another request: it does not open, so no entry (an entry without pw means a blank login).
	st.passwrds[string(target)] = RequestedPassword{RequestID: uuid.New(), Ver: 1, CT: ct, Nonce: nonce}
	if _, ok := ownersFor(t, svc, d, rows)[hex.EncodeToString(target)]; ok {
		t.Fatal("entry kept")
	}
	// A bad observer key cannot be sealed to.
	st.passwrds[string(target)] = RequestedPassword{RequestID: id, Ver: 1, CT: ct, Nonce: nonce}
	bad := device{pub: bytes.Repeat([]byte{0}, 31)}
	if _, ok := ownersFor(t, svc, bad, rows)[hex.EncodeToString(target)]; ok {
		t.Fatal("entry kept for a bad key")
	}
}

func TestOwnersWithRequestsOffDropsRequestedEntries(t *testing.T) {
	svc, _, _ := claimService(t)
	svc.SetRequests(nil, nil)
	target := bytes.Repeat([]byte{1}, 32)
	rows := []OwnerRow{{Target: target, Rank: 0, Requested: true, PWOK: true}}
	if _, ok := ownersFor(t, svc, newDevice(t), rows)[hex.EncodeToString(target)]; ok {
		t.Fatal("requested entry served with requests off")
	}
}

func TestOwnersAnswerWithEightEnvelopesFitsTheCap(t *testing.T) {
	svc, st, sealer := claimService(t)
	d := newDevice(t)
	c := policyCfg()
	c.MQTTHosts = nil
	for i := 0; i < 4; i++ {
		c.MQTTHosts = append(c.MQTTHosts, strings.Repeat(string(rune('a'+i)), 63))
	}
	for i := 0; i < 16; i++ {
		c.ExcludedNodes = append(c.ExcludedNodes, key(byte(i+1)))
	}
	var rows []OwnerRow
	ts := policyNow
	for i := 0; i < 8; i++ {
		target, id := requestedTarget(t, st, sealer, byte(0x80+i), "123456789012345", i)
		ct, nonce, _ := sealer.Seal(id, target, []byte("123456789012345"))
		st.passwrds[string(target)] = RequestedPassword{RequestID: id, Ver: 4000000000, CT: ct, Nonce: nonce}
		rows = append(rows, OwnerRow{Target: target, Rank: 2, LatestTS: &ts, Requested: true, PWOK: true})
	}
	rows = st.withPasswords(rows)
	ans := BuildOwners(rows, BuildPolicy(c, RuntimeState{}, Caller{}, policyNow))
	svc.addEnvelopes(context.Background(), d.pub, rows, ans.Nodes)
	envs := 0
	for _, e := range ans.Nodes {
		if e.PW != nil {
			envs++
		}
	}
	b, err := json.Marshal(ans)
	if err != nil || envs != 8 {
		t.Fatalf("%d envelopes, %v", envs, err)
	}
	if len(b) >= 6144 {
		t.Fatalf("answer is %d bytes", len(b))
	}
}

func TestClaimReportsFromObserversNotHoldingTheTargetAreIgnored(t *testing.T) {
	ctx := context.Background()
	svc, st, sealer := claimService(t)
	target, id := requestedTarget(t, st, sealer, 1, "guest", 0)
	holder, stranger := bytes.Repeat([]byte{0xA}, 32), bytes.Repeat([]byte{0xB}, 32)
	st.hold(target, holder, 0)
	hour := claimNow.Add(time.Hour)
	for _, state := range []string{"admin_stop", "login_failed", "no_reply", "not_direct", "ok"} {
		reportClaim(ctx, svc, claimOf(stranger, ClaimNode{Target: target, Requested: true, State: state}), hour)
	}
	s := st.states[string(target)]
	r := s.Requests[0]
	if r.ID != id || r.Status != StatusWaiting || !r.HasPassword || r.PasswordCT == nil || r.PasswordFailed {
		t.Fatalf("an observer that was never handed the repeater changed the request: %+v", r)
	}
	if len(s.AdminStops) != 0 || len(s.OtherSeen) != 0 || s.NoPasswordBlocked || s.Ver != 1 {
		t.Fatalf("state changed: %+v", s)
	}
	// The holder's report still lands.
	reportClaim(ctx, svc, claimOf(holder, ClaimNode{Target: target, Requested: true, State: "admin_stop"}), hour)
	if st.states[string(target)].Requests[0].Status != StatusAdminStopped {
		t.Fatal("the holder's admin_stop was ignored")
	}
}

func TestRequestSeatClampsTheClaim(t *testing.T) {
	huge := Candidate{RequestSlots: 1000, Held: 1000, SeatsElsewhere: 8}
	if requestSeat(huge) {
		t.Fatal("a claim cannot offer more than the firmware's 8 seats")
	}
	if !requestSeat(Candidate{RequestSlots: 1000, SeatsElsewhere: 7}) {
		t.Fatal("8 seats less 7 held elsewhere leaves one")
	}
}
