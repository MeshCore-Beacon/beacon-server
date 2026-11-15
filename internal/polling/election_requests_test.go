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

func TestDecideRequestsGoOnlyToApprovedObserversByDefault(t *testing.T) {
	ok := rcand('A', 1)
	ok.Approved = true
	in := ElectionInput{Requested: true, RequestCands: []Candidate{rcand('B', 9), ok}}
	// The zero config and the resolved default both mean approved; only an explicit all lifts it.
	for name, cfg := range map[string]config.ResolvedPolling{"unset": {}, "approved": {RequestObservers: config.RequestObserversApproved}} {
		if got := string(order(Decide(in, cfg).Assignments)); got != "A" {
			t.Errorf("%s: %q", name, got)
		}
	}
	if got := string(order(Decide(in, allObservers).Assignments)); got != "BA" {
		t.Errorf("all: %q", got)
	}
	// Evidence is still required with all.
	noEv := rcand('C', 1)
	noEv.EvidenceSNR, noEv.EvidenceCount = nil, 0
	if got := Decide(ElectionInput{Requested: true, RequestCands: []Candidate{noEv}}, allObservers); len(got.Assignments) != 0 {
		t.Errorf("no evidence: %+v", got.Assignments)
	}
}
