// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

var allObservers = config.ResolvedPolling{RequestObservers: config.RequestObserversAll}

func rcand(key byte, snr float64) Candidate {
	return Candidate{Observer: []byte{key}, EvidenceSNR: f64(snr), EvidenceCount: 3, RequestSlots: 2}
}

func TestElectRequestedRules(t *testing.T) {
	noSeat := rcand('C', 9)
	noSeat.RequestSlots = 1
	noSeat.SeatsElsewhere = 1
	holds := rcand('D', 1)
	holds.RequestSlots, holds.Held, holds.SeatsElsewhere = 0, 1, 0 // full, but this target is one it holds
	stopped := rcand('E', 8)
	stopped.AdminStopped = true
	blocked := rcand('F', 8)
	blocked.Blocked = true
	noEv := rcand('G', 8)
	noEv.EvidenceSNR, noEv.EvidenceCount = nil, 0
	notDirect := rcand('H', 8)
	notDirect.State = "not_direct"
	approved := rcand('I', 2)
	approved.Approved = true

	tests := []struct {
		name     string
		cands    []Candidate
		approved bool
		want     string
		wait     string
	}{
		{"ranked by evidence", []Candidate{rcand('A', 2), rcand('B', 7)}, false, "BA", ""},
		{"a full observer is skipped", []Candidate{noSeat, rcand('A', 2)}, false, "A", ""},
		{"an observer holding the target keeps its seat", []Candidate{holds}, false, "D", ""},
		{"admin stop, blocked, no evidence, not direct", []Candidate{stopped, blocked, noEv, notDirect, rcand('A', 1)}, false, "A", ""},
		{"approved only", []Candidate{rcand('A', 9), approved}, true, "I", ""},
		{"nobody eligible", []Candidate{stopped, noEv}, false, "", ReasonNoObserver},
		{"everybody full", []Candidate{noSeat}, false, "", ReasonNoSeat},
		{"no candidates", nil, false, "", ReasonNoObserver},
		{"poller and two standbys at most", []Candidate{rcand('A', 5), rcand('B', 4), rcand('C', 3), rcand('D', 2)}, false, "ABC", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, wait := ElectRequested(tc.cands, nil, tc.approved)
			if got := string(order(out)); got != tc.want || wait != tc.wait {
				t.Fatalf("order %q wait %q, want %q %q", got, wait, tc.want, tc.wait)
			}
			for _, a := range out {
				if !a.Requested {
					t.Fatalf("assignment not marked requested: %+v", a)
				}
			}
		})
	}
}

func TestDecideLocalClaimantKeepsTheTarget(t *testing.T) {
	local := Candidate{Observer: []byte{'L'}, EvidenceSNR: f64(1), EvidenceCount: 1}
	held := rcand('R', 9)
	held.Requested = true
	in := ElectionInput{Cands: []Candidate{local, held}, Requested: true, RequestCands: []Candidate{held, rcand('Q', 8)}}
	el := Decide(in, allObservers)
	if string(order(el.Assignments)) != "L" || el.Assignments[0].Requested {
		t.Fatalf("a local claimant must keep it as a local target: %+v", el.Assignments)
	}
	in.Cands = []Candidate{held}
	el = Decide(in, allObservers)
	if string(order(el.Assignments)) != "RQ" && string(order(el.Assignments)) != "QR" {
		t.Fatalf("no local claimant: %+v", el.Assignments)
	}
	if string(order(el.Assignments)) != "RQ" || !el.Assignments[0].Requested {
		t.Fatalf("best evidence polls: %+v", el.Assignments)
	}
}

func TestDecideDropsRequestedClaimsOfAnUnrequestedTarget(t *testing.T) {
	stale := Candidate{Observer: []byte{'S'}, Requested: true, EvidenceSNR: f64(5), EvidenceCount: 2}
	if el := Decide(ElectionInput{Cands: []Candidate{stale}}, allObservers); len(el.Assignments) != 0 {
		t.Fatalf("%+v", el.Assignments)
	}
}

func TestDecideRequestObserversApproved(t *testing.T) {
	cfg := config.ResolvedPolling{RequestObservers: config.RequestObserversApproved}
	ok := rcand('A', 1)
	ok.Approved = true
	el := Decide(ElectionInput{Requested: true, RequestCands: []Candidate{rcand('B', 9), ok}}, cfg)
	if string(order(el.Assignments)) != "A" {
		t.Fatalf("%+v", el.Assignments)
	}
	// Local targets are unaffected by request_observers.
	local := Candidate{Observer: []byte{'L'}, EvidenceSNR: f64(1), EvidenceCount: 1}
	if el := Decide(ElectionInput{Cands: []Candidate{local}}, cfg); string(order(el.Assignments)) != "L" {
		t.Fatalf("%+v", el.Assignments)
	}
}

func TestDecideRequestsNeedApprovalUnlessResolvedToAll(t *testing.T) {
	ok := rcand('A', 1)
	ok.Approved = true
	in := ElectionInput{Requested: true, RequestCands: []Candidate{rcand('B', 9), ok}}
	// The gate is closed unless the resolved value is all: an unresolved or unknown value needs approval.
	for name, cfg := range map[string]config.ResolvedPolling{
		"unresolved": {}, "approved": {RequestObservers: config.RequestObserversApproved}, "unknown": {RequestObservers: "some"},
	} {
		if got := string(order(Decide(in, cfg).Assignments)); got != "A" {
			t.Errorf("%s: %q", name, got)
		}
	}
	resolved, err := config.PollingConfig{}.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	for name, cfg := range map[string]config.ResolvedPolling{"resolved default": resolved, "all": allObservers} {
		if got := string(order(Decide(in, cfg).Assignments)); got != "BA" {
			t.Errorf("%s: %q", name, got)
		}
	}
	// Evidence is still required with all.
	noEv := rcand('C', 1)
	noEv.EvidenceSNR, noEv.EvidenceCount = nil, 0
	if got := Decide(ElectionInput{Requested: true, RequestCands: []Candidate{noEv}}, allObservers); len(got.Assignments) != 0 {
		t.Errorf("no evidence: %+v", got.Assignments)
	}
}

func TestFreeRequestSeats(t *testing.T) {
	cases := []struct {
		name                              string
		open                              bool
		slots, max, named, pending, wants int
	}{
		{"claim's own number", true, 3, 0, 2, 0, 3},
		{"held to what the limit leaves", true, 7, 0, 5, 0, 3},
		{"more targets than the limit", true, 2, 0, 9, 0, 0},
		{"negative slots", true, -4, 0, 0, 0, 0},
		{"a seat handed out, not yet claimed", true, 3, 0, 2, 1, 2},
		{"every seat handed out", true, 3, 0, 2, 3, 0},
		{"more handed out than offered", true, 1, 0, 2, 4, 0},
		{"expired or not taking requests", false, 5, 0, 1, 0, 0},
		{"a larger limit of its own", true, 4, 12, 8, 0, 4},
		{"a smaller limit of its own", true, 4, 4, 3, 0, 1},
		{"a limit past the ceiling", true, 40, 100, 0, 0, SlotCeiling},
	}
	for _, c := range cases {
		if got := FreeRequestSeats(c.open, c.slots, c.max, c.named, c.pending); got != c.wants {
			t.Errorf("%s: got %d, want %d", c.name, got, c.wants)
		}
	}
}

func TestRequestCapacityFollowsClaimLimit(t *testing.T) {
	for _, c := range []struct{ slots, held, max, want int }{
		{6, 6, 0, 8},    // no limit reported: the default
		{6, 6, 12, 12},  // a larger limit
		{6, 2, 4, 4},    // a smaller one
		{50, 0, 99, 32}, // never past the ceiling
	} {
		if got := RequestCapacity(Candidate{RequestSlots: c.slots, Held: c.held, MaxSlots: c.max}); got != c.want {
			t.Errorf("%+v: got %d", c, got)
		}
	}
}

func TestOfferSeatsFollowsClaimLimit(t *testing.T) {
	rows := make([]OwnerRow, 10)
	for i := range rows {
		rows[i] = OwnerRow{Rank: 0, Requested: true, TargetRequested: true, AcceptsRequests: true, RequestSlots: 10, MaxSlots: 12}
	}
	OfferSeats(rows)
	for i, r := range rows {
		if !r.PWOK {
			t.Fatalf("row %d not offered under a limit of 12", i)
		}
	}
}
