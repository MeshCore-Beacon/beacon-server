// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/google/uuid"
)

type fakeClaim struct {
	ts, recv time.Time
	nodes    map[string]ClaimNode
}

type fakeReading struct {
	target, observer string
	sampled          time.Time
}

type fakeStore struct {
	mu          sync.Mutex
	ids         map[string]uuid.UUID
	claims      map[string]*fakeClaim
	readings    map[string]fakeReading
	assigns     map[string][]Assignment
	evidence    map[string]float64 // observer hex -> SNR
	blocked     map[string]bool
	approved    map[string]bool
	paused      bool
	excluded    [][]byte
	stateErr    error
	flagsErr    error
	recvErr     error
	ownersErr   error
	claimCalls  int
	electCalls  map[string]int // target hex -> ReplacePollingAssignments calls
	idCalls     int
	targetsFail map[string]bool
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		ids: map[string]uuid.UUID{}, claims: map[string]*fakeClaim{}, readings: map[string]fakeReading{},
		assigns: map[string][]Assignment{}, evidence: map[string]float64{}, blocked: map[string]bool{},
		approved:   map[string]bool{},
		electCalls: map[string]int{}, targetsFail: map[string]bool{},
	}
}

func hx(b []byte) string { return hex.EncodeToString(b) }

func (f *fakeStore) ObserverID(_ context.Context, pk []byte) (uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.idCalls++
	id, ok := f.ids[hx(pk)]
	if !ok {
		id = uuid.New()
		f.ids[hx(pk)] = id
	}
	return id, nil
}

func (f *fakeStore) ReplacePollingClaim(_ context.Context, c Claim, _ uuid.UUID, at time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claimCalls++
	old := f.claims[hx(c.Observer)]
	if old != nil && !c.Timestamp.After(old.ts) {
		return false, nil
	}
	fc := &fakeClaim{ts: c.Timestamp, recv: at, nodes: map[string]ClaimNode{}}
	for _, n := range c.Nodes {
		fc.nodes[hx(n.Target)] = n
	}
	f.claims[hx(c.Observer)] = fc
	return true, nil
}

func (f *fakeStore) PollingClaimReceivedAt(_ context.Context, obs []byte) (*time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recvErr != nil {
		return nil, f.recvErr
	}
	c := f.claims[hx(obs)]
	if c == nil {
		return nil, nil
	}
	t := c.recv
	return &t, nil
}

func (f *fakeStore) live(obs, target string, after time.Time) bool {
	c := f.claims[obs]
	if c == nil || !c.recv.After(after) {
		return false
	}
	_, ok := c.nodes[target]
	return ok
}

func (f *fakeStore) InsertRepeaterReading(_ context.Context, r Reading, _ uuid.UUID, _ time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	tag := r.SampledAt.Unix()
	if r.Tag != nil {
		tag = int64(*r.Tag)
	}
	key := fmt.Sprintf("%x|%x|%d|%s", r.Target, r.Observer, tag, r.Method)
	if _, dup := f.readings[key]; dup {
		return false, nil
	}
	f.readings[key] = fakeReading{target: hx(r.Target), observer: hx(r.Observer), sampled: r.SampledAt}
	return true, nil
}

func (f *fakeStore) HasLivePollingCandidate(_ context.Context, o, t []byte, after time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.live(hx(o), hx(t), after), nil
}

func (f *fakeStore) ListPollingTargets(_ context.Context, after time.Time) ([][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := map[string]bool{}
	for t := range f.assigns {
		seen[t] = true
	}
	for _, c := range f.claims {
		if c.recv.After(after) {
			for t := range c.nodes {
				seen[t] = true
			}
		}
	}
	var out [][]byte
	for t := range seen {
		b, _ := hex.DecodeString(t)
		out = append(out, b)
	}
	return out, nil
}

func (f *fakeStore) candidates(target []byte, after time.Time) []Candidate {
	var out []Candidate
	for obs := range f.claims {
		if !f.live(obs, hx(target), after) {
			continue
		}
		b, _ := hex.DecodeString(obs)
		n := f.claims[obs].nodes[hx(target)]
		cand := Candidate{Observer: b, Blocked: f.blocked[obs], Approved: f.approved[obs],
			ClaimedSNR: n.ClaimedSNR, State: n.State, Misses: n.Misses}
		if v, ok := f.evidence[obs]; ok {
			cand.EvidenceSNR = &v
			cand.EvidenceCount = 5
		}
		out = append(out, cand)
	}
	return out
}

func (f *fakeStore) ElectPollingTarget(_ context.Context, target []byte, after, _, _ time.Time,
	decide func(ElectionInput) Election) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.targetsFail[hx(target)] {
		return fmt.Errorf("boom")
	}
	var cur []byte
	for _, a := range f.assigns[hx(target)] {
		if a.Rank == 0 {
			cur = a.Observer
		}
	}
	a := decide(ElectionInput{Cands: f.candidates(target, after), Current: cur}).Assignments
	f.electCalls[hx(target)]++
	if len(a) == 0 {
		delete(f.assigns, hx(target))
		return nil
	}
	f.assigns[hx(target)] = a
	return nil
}

func (f *fakeStore) PollingOwnerRows(_ context.Context, obs []byte, after time.Time) ([]OwnerRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ownersErr != nil {
		return nil, f.ownersErr
	}
	c := f.claims[hx(obs)]
	if c == nil || !c.recv.After(after) {
		return nil, nil
	}
	var out []OwnerRow
	for t := range c.nodes {
		tb, _ := hex.DecodeString(t)
		row := OwnerRow{Target: tb, Rank: -1, Assigned: len(f.assigns[t]),
			ObserverApproved: f.approved[hx(obs)], ObserverBlocked: f.blocked[hx(obs)]}
		trusted := map[string]bool{}
		for _, a := range f.assigns[t] {
			if bytes.Equal(a.Observer, obs) {
				row.Rank = a.Rank
			}
			trusted[hx(a.Observer)] = a.EvidenceCount > 0
		}
		for _, r := range f.readings {
			if r.target == t && trusted[r.observer] && (row.LatestTS == nil || r.sampled.After(*row.LatestTS)) {
				s := r.sampled
				row.LatestTS = &s
			}
		}
		out = append(out, row)
	}
	return out, nil
}

func (f *fakeStore) PollingObserverFlags(_ context.Context, obs []byte) (bool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.flagsErr != nil {
		return false, false, f.flagsErr
	}
	if f.stateErr != nil {
		return false, false, f.stateErr
	}
	return f.blocked[hx(obs)], f.approved[hx(obs)], nil
}

func (f *fakeStore) PollingState(context.Context) (bool, *time.Time, [][]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stateErr != nil {
		return false, nil, nil, f.stateErr
	}
	return f.paused, nil, f.excluded, nil
}

func (f *fakeStore) readingCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.readings)
}

var t0 = time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newSvc(t *testing.T, enabled bool) (*Service, *fakeStore, *clock) {
	t.Helper()
	st := newFakeStore()
	ck := &clock{t: t0}
	cfg := config.ResolvedPolling{Enabled: enabled, MQTTHosts: []string{"mqtt.example"},
		DirectWindow: time.Hour, Grace: 30 * time.Minute, MaxRequests: 2}
	return NewService(st, cfg, ck.now, slog.New(slog.NewTextHandler(io.Discard, nil))), st, ck
}

func newSvcAuthority(t *testing.T, authority string) (*Service, *fakeStore, *clock) {
	t.Helper()
	s, st, ck := newSvc(t, true)
	s.cfg.Authority = authority
	return s, st, ck
}

func k32(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

func mustOwners(t *testing.T, s *Service, c Caller) OwnersAnswer {
	t.Helper()
	a, err := s.Owners(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func mustPolicy(t *testing.T, s *Service, c Caller) Policy {
	t.Helper()
	p, err := s.Policy(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func claimJSON(obs []byte, ts time.Time, targets ...[]byte) []byte {
	nodes := []map[string]any{}
	for _, tg := range targets {
		nodes = append(nodes, map[string]any{"node_id": hx(tg), "key": "own"})
	}
	b, _ := json.Marshal(map[string]any{"v": 2, "origin": "obs", "origin_id": hx(obs),
		"timestamp": ts.Format(time.RFC3339), "plugin": "fw", "nodes": nodes})
	return b
}

func claimMissesJSON(obs []byte, ts time.Time, target []byte, misses int) []byte {
	b, _ := json.Marshal(map[string]any{"v": 2, "origin": "obs", "origin_id": hx(obs),
		"timestamp": ts.Format(time.RFC3339), "plugin": "fw",
		"nodes": []map[string]any{{"node_id": hx(target), "key": "own", "misses": misses}}})
	return b
}

func readingJSON(obs, target []byte, sampled time.Time, extra ...int) []byte {
	tag := 1
	if len(extra) > 0 {
		tag = extra[0]
	}
	b, _ := json.Marshal(map[string]any{"v": 2, "origin_id": hx(obs), "plugin": "fw",
		"readings": []map[string]any{{"node_id": hx(target), "method": "status",
			"sampled_at": sampled.Format(time.RFC3339), "tag": tag, "battery_mv": 4000}}})
	return b
}

func TestHandleClaimStoresAndElects(t *testing.T) {
	s, st, _ := newSvc(t, true)
	a, b, o := k32(0xa), k32(0xb), k32(1)
	s.HandleClaim(context.Background(), hx(o), claimJSON(o, t0, a, b))
	if st.claimCalls != 1 {
		t.Fatalf("claim calls = %d", st.claimCalls)
	}
	if st.electCalls[hx(a)] != 1 || st.electCalls[hx(b)] != 1 {
		t.Fatalf("elect calls = %v", st.electCalls)
	}
	if len(st.assigns[hx(a)]) != 1 || st.assigns[hx(a)][0].Rank != 0 {
		t.Fatalf("assigns = %v", st.assigns[hx(a)])
	}
}

func TestHandleClaimTopicMismatch(t *testing.T) {
	s, st, _ := newSvc(t, true)
	o := k32(1)
	s.HandleClaim(context.Background(), hx(k32(2)), claimJSON(o, t0, k32(0xa)))
	if st.claimCalls != 0 || st.idCalls != 0 {
		t.Fatal("mismatched claim was stored")
	}
	s.HandleClaim(context.Background(), strings.ToUpper(hx(o)), claimJSON(o, t0, k32(0xa)))
	if st.claimCalls != 1 {
		t.Fatal("upper-case topic key should match")
	}
}

func TestHandleClaimFutureTimestamp(t *testing.T) {
	s, st, _ := newSvc(t, true)
	o := k32(1)
	s.HandleClaim(context.Background(), hx(o), claimJSON(o, t0.Add(11*time.Minute), k32(0xa)))
	if st.claimCalls != 0 {
		t.Fatal("future claim stored")
	}
	s.HandleClaim(context.Background(), hx(o), claimJSON(o, t0.Add(9*time.Minute), k32(0xa)))
	if st.claimCalls != 1 {
		t.Fatal("near-future claim dropped")
	}
}

func TestHandleClaimStaleIsIgnored(t *testing.T) {
	s, st, ck := newSvc(t, true)
	a, b, o := k32(0xa), k32(0xb), k32(1)
	s.HandleClaim(context.Background(), hx(o), claimJSON(o, t0, b))
	elA := st.electCalls[hx(a)]
	ck.t = t0.Add(ClaimMinInterval)
	s.HandleClaim(context.Background(), hx(o), claimJSON(o, t0.Add(-time.Minute), a, b))
	if st.claimCalls != 2 {
		t.Fatalf("claim calls = %d", st.claimCalls)
	}
	if st.electCalls[hx(a)] != elA {
		t.Fatal("election ran for a stale claim")
	}
	if _, ok := st.claims[hx(o)].nodes[hx(a)]; ok {
		t.Fatal("stale claim replaced candidates")
	}
}

func TestHandleClaimRemovedTargetIsReElected(t *testing.T) {
	s, st, ck := newSvc(t, true)
	a, b, o1, o2 := k32(0xa), k32(0xb), k32(1), k32(2)
	s.HandleClaim(context.Background(), hx(o2), claimJSON(o2, t0, a))
	s.HandleClaim(context.Background(), hx(o1), claimJSON(o1, t0, a, b))
	before := st.electCalls[hx(a)]
	ck.t = t0.Add(time.Minute)
	s.HandleClaim(context.Background(), hx(o1), claimJSON(o1, t0.Add(time.Second), b))
	if st.electCalls[hx(a)] != before+1 {
		t.Fatalf("a not re-elected once: %d -> %d", before, st.electCalls[hx(a)])
	}
	if got := st.assigns[hx(a)]; len(got) != 1 || !bytes.Equal(got[0].Observer, o2) {
		t.Fatalf("assigns = %v", got)
	}
}

func TestHandleClaimRateLimited(t *testing.T) {
	s, st, ck := newSvc(t, true)
	a, b, o := k32(0xa), k32(0xb), k32(1)
	ctx := context.Background()
	s.HandleClaim(ctx, hx(o), claimJSON(o, t0, a))
	ids, elect := st.idCalls, st.electCalls[hx(b)]
	ck.t = t0.Add(ClaimMinInterval - time.Second)
	s.HandleClaim(ctx, hx(o), claimJSON(o, ck.t, b))
	if st.claimCalls != 1 || st.idCalls != ids || st.electCalls[hx(b)] != elect {
		t.Fatal("claim inside the interval caused writes")
	}
	ck.t = t0.Add(ClaimMinInterval)
	s.HandleClaim(ctx, hx(o), claimJSON(o, ck.t, b))
	if st.claimCalls != 2 || st.electCalls[hx(b)] != elect+1 {
		t.Fatal("claim after the interval dropped")
	}
	st.recvErr = fmt.Errorf("db down")
	ck.t = t0.Add(time.Hour)
	s.HandleClaim(ctx, hx(o), claimJSON(o, ck.t, a))
	if st.claimCalls != 2 {
		t.Fatal("claim accepted while the last receive time could not be read")
	}
}

func TestHandleReadingDuplicate(t *testing.T) {
	s, st, _ := newSvc(t, true)
	o, x := k32(1), k32(0xa)
	ctx := context.Background()
	st.evidence[hx(o)] = 5 // only an evidenced poller's readings set latest_ts
	s.HandleClaim(ctx, hx(o), claimJSON(o, t0, x))
	at := t0.Add(-10 * time.Minute)
	s.HandleReading(ctx, hx(o), readingJSON(o, x, at))
	first := mustOwners(t, s, Caller{Observer: o}).Nodes[hx(x)].LatestTS
	s.HandleReading(ctx, hx(o), readingJSON(o, x, at))
	if st.readingCount() != 1 {
		t.Fatalf("rows = %d", st.readingCount())
	}
	second := mustOwners(t, s, Caller{Observer: o}).Nodes[hx(x)].LatestTS
	if first == nil || second == nil || *first != *second {
		t.Fatalf("latest ts changed: %v %v", first, second)
	}
}

func TestHandleReadingNeedsLiveClaim(t *testing.T) {
	s, st, ck := newSvc(t, true)
	o, x, y := k32(1), k32(0xa), k32(0xb)
	ctx := context.Background()
	s.HandleReading(ctx, hx(o), readingJSON(o, x, t0))
	if st.readingCount() != 0 {
		t.Fatal("stored without claim")
	}
	s.HandleClaim(ctx, hx(o), claimJSON(o, t0, x))
	s.HandleReading(ctx, hx(o), readingJSON(o, y, t0))
	if st.readingCount() != 0 {
		t.Fatal("stored for an unclaimed target")
	}
	s.HandleReading(ctx, hx(o), readingJSON(o, x, t0, 2))
	if st.readingCount() != 1 {
		t.Fatal("claimed target dropped")
	}
	ck.t = t0.Add(ClaimTTL + time.Second)
	s.HandleReading(ctx, hx(o), readingJSON(o, x, ck.t, 3))
	if st.readingCount() != 1 {
		t.Fatal("stored against an expired claim")
	}
}

func TestHandleReadingTimeBounds(t *testing.T) {
	s, st, _ := newSvc(t, true)
	o, x := k32(1), k32(0xa)
	ctx := context.Background()
	s.HandleClaim(ctx, hx(o), claimJSON(o, t0, x))
	s.HandleReading(ctx, hx(o), readingJSON(o, x, t0.Add(61*time.Second), 1))
	s.HandleReading(ctx, hx(o), readingJSON(o, x, t0.Add(-2*time.Hour-time.Minute), 2))
	if st.readingCount() != 0 {
		t.Fatal("out of bounds reading stored")
	}
	s.HandleReading(ctx, hx(o), readingJSON(o, x, t0.Add(30*time.Second), 3))
	s.HandleReading(ctx, hx(o), readingJSON(o, x, t0.Add(-time.Hour-59*time.Minute), 4))
	s.HandleReading(ctx, hx(o), readingJSON(o, x, t0, 5))
	if st.readingCount() != 3 {
		t.Fatalf("rows = %d, want 3", st.readingCount())
	}
}

func TestHandleReadingTopicMismatch(t *testing.T) {
	s, st, _ := newSvc(t, true)
	o, x := k32(1), k32(0xa)
	ctx := context.Background()
	s.HandleClaim(ctx, hx(o), claimJSON(o, t0, x))
	ids := st.idCalls
	s.HandleReading(ctx, hx(k32(2)), readingJSON(o, x, t0))
	if st.readingCount() != 0 || st.idCalls != ids {
		t.Fatal("mismatched reading was handled")
	}
}

func TestHandleReadingMultipleCallsObserverIDOnce(t *testing.T) {
	s, st, _ := newSvc(t, true)
	o, x := k32(1), k32(0xa)
	ctx := context.Background()
	s.HandleClaim(ctx, hx(o), claimJSON(o, t0, x))
	ids := st.idCalls
	raw, _ := json.Marshal(map[string]any{"v": 2, "origin_id": hx(o), "readings": []map[string]any{
		{"node_id": hx(x), "method": "status", "sampled_at": t0.Format(time.RFC3339), "tag": 1},
		{"node_id": hx(x), "method": "status", "sampled_at": t0.Format(time.RFC3339), "tag": 2},
	}})
	s.HandleReading(ctx, hx(o), raw)
	if st.readingCount() != 2 || st.idCalls != ids+1 {
		t.Fatalf("rows=%d idCalls=%d", st.readingCount(), st.idCalls-ids)
	}
}

func TestHandleReadingTargetWithoutNodeRow(t *testing.T) {
	// Readings are keyed by public key, so a repeater with no node row is still accepted.
	s, st, _ := newSvc(t, true)
	o, x := k32(1), k32(0xee)
	ctx := context.Background()
	s.HandleClaim(ctx, hx(o), claimJSON(o, t0, x))
	s.HandleReading(ctx, hx(o), readingJSON(o, x, t0))
	if st.readingCount() != 1 {
		t.Fatal("reading for an unknown node dropped")
	}
}

func TestOwnersUnknownObserver(t *testing.T) {
	s, _, _ := newSvc(t, true)
	for _, c := range []Caller{{}, {Observer: k32(9)}} {
		a := mustOwners(t, s, c)
		if a.Nodes == nil || len(a.Nodes) != 0 {
			t.Fatalf("nodes = %v", a.Nodes)
		}
		if a.Policy.Paused || len(a.Policy.MQTTHosts) != 1 {
			t.Fatalf("policy = %+v", a.Policy)
		}
	}
}

func TestOwnersBlockedObserver(t *testing.T) {
	s, st, _ := newSvc(t, true)
	o, x := k32(1), k32(0xa)
	ctx := context.Background()
	s.HandleClaim(ctx, hx(o), claimJSON(o, t0, x))
	st.blocked[hx(o)] = true
	a := mustOwners(t, s, Caller{Observer: o})
	if !a.Policy.ObserverBlocked || a.Nodes == nil || len(a.Nodes) != 0 {
		t.Fatalf("answer = %+v", a)
	}
}

func TestOwnersReflectsElection(t *testing.T) {
	s, st, _ := newSvc(t, true)
	o1, o2, x := k32(1), k32(2), k32(0xa)
	ctx := context.Background()
	st.evidence[hx(o1)] = 9
	st.evidence[hx(o2)] = 2
	s.HandleClaim(ctx, hx(o2), claimJSON(o2, t0, x))
	s.HandleClaim(ctx, hx(o1), claimJSON(o1, t0, x))
	e1 := mustOwners(t, s, Caller{Observer: o1}).Nodes[hx(x)]
	e2 := mustOwners(t, s, Caller{Observer: o2}).Nodes[hx(x)]
	if e1.Role != "polling" || !e1.Mine {
		t.Fatalf("o1 = %+v", e1)
	}
	if e2.Role != "standby" || e2.StandbyRank != 1 {
		t.Fatalf("o2 = %+v", e2)
	}
}

func TestOwnersBeforeFirstElection(t *testing.T) {
	s, st, _ := newSvc(t, true)
	o, x := k32(1), k32(0xa)
	st.claims[hx(o)] = &fakeClaim{ts: t0, recv: t0, nodes: map[string]ClaimNode{hx(x): {Target: x}}}
	e := mustOwners(t, s, Caller{Observer: o}).Nodes[hx(x)]
	if e.Role != "polling" || !e.Mine {
		t.Fatalf("entry = %+v", e)
	}
}

func TestOwnersUnassignedCandidateIsLastStandby(t *testing.T) {
	s, st, _ := newSvc(t, true)
	o1, o2, o3, x := k32(1), k32(2), k32(3), k32(0xa)
	ctx := context.Background()
	s.HandleClaim(ctx, hx(o1), claimJSON(o1, t0, x))
	s.HandleClaim(ctx, hx(o2), claimJSON(o2, t0, x))
	// A live claim the election has not placed yet, or left out (admin_stop, not_direct, blocked).
	st.claims[hx(o3)] = &fakeClaim{ts: t0, recv: t0, nodes: map[string]ClaimNode{hx(x): {Target: x}}}
	e := mustOwners(t, s, Caller{Observer: o3}).Nodes[hx(x)]
	if e.Role != "standby" || e.Mine || e.StandbyRank != 2 {
		t.Fatalf("entry = %+v", e)
	}
	polling := 0
	for _, o := range [][]byte{o1, o2, o3} {
		if e := mustOwners(t, s, Caller{Observer: o}).Nodes[hx(x)]; e.Role == "polling" && e.Mine {
			polling++
		}
	}
	if polling != 1 {
		t.Fatalf("%d observers told they poll", polling)
	}
}

func TestOwnersExcludedTarget(t *testing.T) {
	s, st, _ := newSvc(t, true)
	o, x := k32(1), k32(0xa)
	ctx := context.Background()
	s.HandleClaim(ctx, hx(o), claimJSON(o, t0, x))
	st.excluded = [][]byte{x}
	e := mustOwners(t, s, Caller{Observer: o}).Nodes[hx(x)]
	if e.Role != "excluded" || !e.Excluded {
		t.Fatalf("entry = %+v", e)
	}
}

func TestPolicyAndOwnersReturnStoreErrors(t *testing.T) {
	o, x := k32(1), k32(0xa)
	ctx := context.Background()
	for name, set := range map[string]func(*fakeStore){
		"state":  func(st *fakeStore) { st.stateErr = fmt.Errorf("db down") },
		"flags":  func(st *fakeStore) { st.flagsErr = fmt.Errorf("db down") },
		"owners": func(st *fakeStore) { st.ownersErr = fmt.Errorf("db down") },
	} {
		s, st, _ := newSvc(t, true)
		s.HandleClaim(ctx, hx(o), claimJSON(o, t0, x))
		set(st)
		_, perr := s.Policy(ctx, Caller{Observer: o})
		_, oerr := s.Owners(ctx, Caller{Observer: o})
		if oerr == nil {
			t.Errorf("%s: owners answered without an error", name)
		}
		if (perr == nil) != (name == "owners") {
			t.Errorf("%s: policy err = %v", name, perr)
		}
	}
	// Without an observer only the runtime state is read.
	s, st, _ := newSvc(t, true)
	st.flagsErr = fmt.Errorf("db down")
	if _, err := s.Policy(ctx, Caller{}); err != nil {
		t.Fatal(err)
	}
}

func TestServiceDisabled(t *testing.T) {
	s, st, _ := newSvc(t, false)
	o, x := k32(1), k32(0xa)
	ctx := context.Background()
	s.HandleClaim(ctx, hx(o), claimJSON(o, t0, x))
	s.HandleReading(ctx, hx(o), readingJSON(o, x, t0))
	if st.claimCalls != 0 || st.readingCount() != 0 {
		t.Fatal("disabled service stored data")
	}
	want := DisabledPolicy()
	if got := mustPolicy(t, s, Caller{Observer: o}); got.Version != want.Version || !got.Paused {
		t.Fatalf("policy = %+v", got)
	}
	a := mustOwners(t, s, Caller{Observer: o})
	if a.Nodes == nil || len(a.Nodes) != 0 || a.Policy.Version != want.Version {
		t.Fatalf("owners = %+v", a)
	}
}

func TestElectAllContinuesAfterFailure(t *testing.T) {
	s, st, _ := newSvc(t, true)
	o, a, b := k32(1), k32(0xa), k32(0xb)
	ctx := context.Background()
	s.HandleClaim(ctx, hx(o), claimJSON(o, t0, a, b))
	st.electCalls = map[string]int{}
	st.targetsFail[hx(a)] = true
	if err := s.ElectAll(ctx); err == nil {
		t.Fatal("want first error")
	}
	if st.electCalls[hx(b)] != 1 {
		t.Fatal("b not elected after a failed")
	}
}

func TestElectTargetClearsWithoutCandidates(t *testing.T) {
	s, st, ck := newSvc(t, true)
	o, x := k32(1), k32(0xa)
	ctx := context.Background()
	s.HandleClaim(ctx, hx(o), claimJSON(o, t0, x))
	ck.t = t0.Add(ClaimTTL + time.Minute)
	if err := s.ElectTarget(ctx, x); err != nil {
		t.Fatal(err)
	}
	if len(st.assigns[hx(x)]) != 0 {
		t.Fatal("assignments not cleared")
	}
}

func TestMalformedPayloadsDoNotPanic(t *testing.T) {
	s, st, _ := newSvc(t, true)
	o := hx(k32(1))
	for _, raw := range [][]byte{nil, []byte("{"), []byte("[]"), []byte("null"), []byte("1"), bytes.Repeat([]byte{'a'}, 1<<20)} {
		s.HandleClaim(context.Background(), o, raw)
		s.HandleReading(context.Background(), o, raw)
	}
	if st.claimCalls != 0 || st.readingCount() != 0 {
		t.Fatal("garbage stored")
	}
}

func TestBlockedObserverClaimDropped(t *testing.T) {
	s, st, _ := newSvc(t, true)
	o, x := k32(1), k32(0xa)
	st.blocked[hx(o)] = true
	s.HandleClaim(context.Background(), hx(o), claimJSON(o, t0, x))
	if st.claimCalls != 0 || st.idCalls != 0 || len(st.electCalls) != 0 {
		t.Fatalf("blocked claim caused writes: %d %d %v", st.claimCalls, st.idCalls, st.electCalls)
	}
}

func TestBlockedObserverReadingDropped(t *testing.T) {
	s, st, _ := newSvc(t, true)
	good, bad, x := k32(1), k32(2), k32(0xa)
	ctx := context.Background()
	st.evidence[hx(good)] = 5
	s.HandleClaim(ctx, hx(good), claimJSON(good, t0, x))
	s.HandleClaim(ctx, hx(bad), claimJSON(bad, t0, x))
	s.HandleReading(ctx, hx(good), readingJSON(good, x, t0.Add(-time.Hour)))
	before := mustOwners(t, s, Caller{Observer: good}).Nodes[hx(x)].LatestTS
	st.blocked[hx(bad)] = true
	ids := st.idCalls
	s.HandleReading(ctx, hx(bad), readingJSON(bad, x, t0))
	after := mustOwners(t, s, Caller{Observer: good}).Nodes[hx(x)].LatestTS
	if st.readingCount() != 1 || st.idCalls != ids || before == nil || after == nil || *before != *after {
		t.Fatalf("blocked reading was stored: rows=%d before=%v after=%v", st.readingCount(), before, after)
	}
}

func TestFlagsErrorDropsMessages(t *testing.T) {
	s, st, _ := newSvc(t, true)
	o, x := k32(1), k32(0xa)
	ctx := context.Background()
	s.HandleClaim(ctx, hx(o), claimJSON(o, t0, x))
	calls, ids := st.claimCalls, st.idCalls
	st.flagsErr = fmt.Errorf("db down")
	s.HandleClaim(ctx, hx(o), claimJSON(o, t0.Add(time.Second), x))
	s.HandleReading(ctx, hx(o), readingJSON(o, x, t0))
	if st.claimCalls != calls || st.readingCount() != 0 || st.idCalls != ids {
		t.Fatal("message accepted while flags lookup failed")
	}
	st.flagsErr = nil
	s.HandleReading(ctx, hx(o), readingJSON(o, x, t0))
	if st.readingCount() != 1 {
		t.Fatal("unblocked observer affected")
	}
}

func TestParseErrorLogHasNoPayload(t *testing.T) {
	s, _, _ := newSvc(t, true)
	var buf bytes.Buffer
	s.log = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	o := k32(1)
	junk := "JUNKSECRET99"
	claim := fmt.Sprintf(`{"v":2,"origin_id":%q,"timestamp":%q,"nodes":[]}`, hx(o), junk)
	reading := fmt.Sprintf(`{"v":2,"origin_id":%q,"readings":[{"node_id":%q,"method":"status","sampled_at":%q}]}`, hx(o), hx(o), junk)
	s.HandleClaim(context.Background(), hx(o), []byte(claim))
	s.HandleReading(context.Background(), hx(o), []byte(reading))
	if buf.Len() == 0 {
		t.Fatal("expected a debug log")
	}
	if strings.Contains(buf.String(), junk) {
		t.Fatalf("payload leaked into log: %s", buf.String())
	}
}

func TestEvidencedFailingPollerOutranksHealthyStranger(t *testing.T) {
	s, st, _ := newSvc(t, true)
	real, stranger, x := k32(1), k32(2), k32(0xa)
	ctx := context.Background()
	st.evidence[hx(real)] = 4
	s.HandleClaim(ctx, hx(real), claimMissesJSON(real, t0, x, MissLimit))
	s.HandleClaim(ctx, hx(stranger), claimMissesJSON(stranger, t0, x, 0))
	if e := mustOwners(t, s, Caller{Observer: real}).Nodes[hx(x)]; e.Role != "polling" || !e.Mine {
		t.Fatalf("evidenced observer = %+v", e)
	}
	if e := mustOwners(t, s, Caller{Observer: stranger}).Nodes[hx(x)]; e.Role != "standby" || e.StandbyRank != 1 {
		t.Fatalf("stranger = %+v", e)
	}
	s.HandleReading(ctx, hx(stranger), readingJSON(stranger, x, t0))
	if st.readingCount() != 1 {
		t.Fatal("stranger's reading not stored")
	}
	for _, o := range [][]byte{real, stranger} {
		if got := mustOwners(t, s, Caller{Observer: o}).Nodes[hx(x)].LatestTS; got != nil {
			t.Fatalf("reading without evidence moved latest_ts to %s", *got)
		}
	}
}

func TestSoleUnevidencedPollerHoldsNobodyBack(t *testing.T) {
	s, st, ck := newSvc(t, true)
	o1, o2, x := k32(1), k32(2), k32(0xa)
	ctx := context.Background()
	s.HandleClaim(ctx, hx(o1), claimJSON(o1, t0, x))
	if e := mustOwners(t, s, Caller{Observer: o1}).Nodes[hx(x)]; e.Role != "polling" || !e.Mine {
		t.Fatalf("sole observer = %+v", e)
	}
	s.HandleReading(ctx, hx(o1), readingJSON(o1, x, t0))
	ck.t = t0.Add(time.Minute)
	s.HandleClaim(ctx, hx(o2), claimJSON(o2, ck.t, x))
	if st.readingCount() != 1 {
		t.Fatal("reading not stored")
	}
	for _, o := range [][]byte{o1, o2} {
		if got := mustOwners(t, s, Caller{Observer: o}).Nodes[hx(x)].LatestTS; got != nil {
			t.Fatalf("latest_ts = %s", *got)
		}
	}
}

func TestApprovedAuthority(t *testing.T) {
	good, best, x := k32(1), k32(2), k32(0xa)
	ctx := context.Background()
	setup := func(authority string) (*Service, *fakeStore, *clock) {
		s, st, ck := newSvcAuthority(t, authority)
		st.evidence[hx(good)] = 2
		st.evidence[hx(best)] = 9
		st.approved[hx(good)] = true
		s.HandleClaim(ctx, hx(good), claimJSON(good, t0, x))
		s.HandleClaim(ctx, hx(best), claimJSON(best, t0, x))
		return s, st, ck
	}

	// Evidence mode is unchanged: the best evidence polls whatever its approval.
	s, _, _ := setup(config.PollingAuthorityEvidence)
	if e := mustOwners(t, s, Caller{Observer: best}).Nodes[hx(x)]; e.Role != "polling" || !e.Mine {
		t.Fatalf("evidence mode: best = %+v", e)
	}

	s, st, ck := setup(config.PollingAuthorityApproved)
	if a := st.assigns[hx(x)]; len(a) != 1 || !bytes.Equal(a[0].Observer, good) {
		t.Fatalf("assigns = %v", a)
	}
	if e := mustOwners(t, s, Caller{Observer: good}).Nodes[hx(x)]; e.Role != "polling" || !e.Mine {
		t.Fatalf("approved = %+v", e)
	}
	if e := mustOwners(t, s, Caller{Observer: best}).Nodes[hx(x)]; e.Role != "standby" || e.Mine || e.StandbyRank != 1 {
		t.Fatalf("unapproved = %+v", e)
	}
	s.HandleReading(ctx, hx(best), readingJSON(best, x, t0))
	if got := mustOwners(t, s, Caller{Observer: good}).Nodes[hx(x)].LatestTS; got != nil || st.readingCount() != 1 {
		t.Fatalf("unapproved reading: latest_ts %v, rows %d", got, st.readingCount())
	}

	// Approval makes it eligible at the next election, where its evidence wins.
	st.approved[hx(best)] = true
	ck.t = t0.Add(time.Minute)
	if err := s.ElectTarget(ctx, x); err != nil {
		t.Fatal(err)
	}
	if e := mustOwners(t, s, Caller{Observer: best}).Nodes[hx(x)]; e.Role != "polling" || !e.Mine {
		t.Fatalf("after approval = %+v", e)
	}
	if got := mustOwners(t, s, Caller{Observer: good}).Nodes[hx(x)].LatestTS; got == nil || *got != t0.Format(time.RFC3339) {
		t.Fatal("approved poller's reading did not count", got)
	}
}

func TestApprovedAuthorityNobodyApprovedFailsOpen(t *testing.T) {
	s, st, _ := newSvcAuthority(t, config.PollingAuthorityApproved)
	o, x := k32(1), k32(0xa)
	s.HandleClaim(context.Background(), hx(o), claimJSON(o, t0, x))
	if len(st.assigns[hx(x)]) != 0 {
		t.Fatal("unapproved observer was assigned")
	}
	if e := mustOwners(t, s, Caller{Observer: o}).Nodes[hx(x)]; e.Role != "polling" || !e.Mine {
		t.Fatalf("entry = %+v", e)
	}
}

func TestPolicyExclusionOverflowIsLogged(t *testing.T) {
	s, st, _ := newSvc(t, true)
	var buf bytes.Buffer
	s.log = slog.New(slog.NewTextHandler(&buf, nil))
	for i := range 16 {
		s.cfg.ExcludedNodes = append(s.cfg.ExcludedNodes, k32(byte(i+1)))
	}
	st.excluded = [][]byte{k32(1), k32(0xee)}
	p := mustPolicy(t, s, Caller{})
	if len(p.ExcludedNodes) != 16 || p.ExcludedNodes[15] != hx(k32(16)) {
		t.Fatalf("excluded = %v", p.ExcludedNodes)
	}
	if !strings.Contains(buf.String(), "level=ERROR") {
		t.Fatalf("overflow not logged: %q", buf.String())
	}
	buf.Reset()
	st.excluded = [][]byte{k32(1)}
	mustPolicy(t, s, Caller{})
	if buf.Len() != 0 {
		t.Fatalf("logged without overflow: %q", buf.String())
	}
}
