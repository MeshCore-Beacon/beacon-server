// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
)

func f64(v float64) *float64 { return &v }
func f32(v float32) *float32 { return &v }

func cand(key byte, snr *float64, n int) Candidate {
	return Candidate{Observer: []byte{key}, EvidenceSNR: snr, EvidenceCount: n}
}

func order(as []Assignment) []byte {
	out := []byte{}
	for _, a := range as {
		out = append(out, a.Observer[0])
	}
	return out
}

func wantOrder(t *testing.T, as []Assignment, want string) {
	t.Helper()
	if got := string(order(as)); got != want {
		t.Fatalf("order = %q, want %q", got, want)
	}
	for i, a := range as {
		if a.Rank != i {
			t.Fatalf("rank[%d] = %d", i, a.Rank)
		}
	}
}

func TestElectRanksByEvidence(t *testing.T) {
	as := Elect([]Candidate{cand('A', f64(4), 5), cand('B', f64(9), 2), cand('C', f64(9), 7)}, nil)
	wantOrder(t, as, "CBA")
	if as[0].EvidenceCount != 7 || *as[0].EvidenceSNR != 9 {
		t.Fatalf("evidence not copied: %+v", as[0])
	}
}

func TestElectTieBreaksOnKeyBytes(t *testing.T) {
	as := Elect([]Candidate{cand('B', f64(5), 3), cand('A', f64(5), 3)}, nil)
	wantOrder(t, as, "AB")
}

func TestElectNoEvidenceGoesLast(t *testing.T) {
	b := cand('B', nil, 0)
	b.ClaimedSNR = f32(12)
	wantOrder(t, Elect([]Candidate{b, cand('A', f64(2), 2)}, nil), "AB")

	x := cand('X', nil, 0)
	y := cand('Y', nil, 0)
	y.ClaimedSNR = f32(-3)
	z := cand('Z', nil, 0)
	z.ClaimedSNR = f32(5)
	wantOrder(t, Elect([]Candidate{x, y, z}, nil), "ZYX")
}

func TestElectDropsIneligible(t *testing.T) {
	a := cand('A', f64(9), 1)
	a.State = "admin_stop"
	b := cand('B', f64(9), 1)
	b.State = "not_direct"
	c := cand('C', f64(9), 1)
	c.Blocked = true
	d := cand('D', f64(1), 1)
	d.State = "something_new"
	e := cand('E', f64(0), 1)
	e.State = "polling"
	wantOrder(t, Elect([]Candidate{a, b, c, d, e}, nil), "DE")
}

func TestElectKeepsCurrentWithinHysteresis(t *testing.T) {
	cur := []byte{'A'}
	wantOrder(t, Elect([]Candidate{cand('A', f64(6), 1), cand('B', f64(8.9), 1)}, cur), "AB")
	wantOrder(t, Elect([]Candidate{cand('A', f64(6), 1), cand('B', f64(9.0), 1)}, cur), "BA")
}

func TestElectKeepsCurrentRestInPlainOrder(t *testing.T) {
	cs := []Candidate{cand('A', f64(1), 1), cand('B', f64(3), 1), cand('C', f64(2), 1)}
	// Plain order is B, C, A; A is within 3 dB of B so it keeps 0.
	wantOrder(t, Elect(cs, []byte{'A'}), "ABC")
}

func TestElectDisplacedCurrentTakesNormalPlace(t *testing.T) {
	cs := []Candidate{cand('A', f64(5), 1), cand('B', f64(20), 1), cand('C', f64(1), 1)}
	wantOrder(t, Elect(cs, []byte{'A'}), "BAC")
}

func TestElectReplacesFailingCurrent(t *testing.T) {
	a := cand('A', f64(10), 1)
	a.Misses = 3
	wantOrder(t, Elect([]Candidate{a, cand('B', f64(2), 1)}, []byte{'A'}), "BA")
}

func TestElectFailingCurrentAloneStays(t *testing.T) {
	a := cand('A', f64(10), 1)
	a.Misses = 5
	wantOrder(t, Elect([]Candidate{a}, []byte{'A'}), "A")
}

func TestElectCurrentEvidenceRules(t *testing.T) {
	// B has evidence, current has none: B wins.
	wantOrder(t, Elect([]Candidate{cand('A', nil, 0), cand('B', f64(-5), 1)}, []byte{'A'}), "BA")
	// Current has evidence, challenger has none: current stays.
	wantOrder(t, Elect([]Candidate{cand('A', f64(-5), 1), cand('B', nil, 0)}, []byte{'A'}), "AB")
	// Neither has evidence: current stays even if challenger claims more.
	b := cand('B', nil, 0)
	b.ClaimedSNR = f32(20)
	wantOrder(t, Elect([]Candidate{cand('A', nil, 0), b}, []byte{'A'}), "AB")
}

func TestElectCurrentGone(t *testing.T) {
	wantOrder(t, Elect([]Candidate{cand('A', f64(1), 1), cand('B', f64(2), 1)}, []byte{'Z'}), "BA")
	wantOrder(t, Elect([]Candidate{cand('A', f64(1), 1), cand('B', f64(2), 1)}, []byte{}), "BA")
}

func TestElectCurrentIneligible(t *testing.T) {
	a := cand('A', f64(9), 1)
	a.Blocked = true
	wantOrder(t, Elect([]Candidate{a, cand('B', f64(1), 1)}, []byte{'A'}), "B")
}

func TestElectEmpty(t *testing.T) {
	if got := Elect(nil, nil); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
	a := cand('A', f64(1), 1)
	a.Blocked = true
	if got := Elect([]Candidate{a}, []byte{'A'}); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}

func TestElectRanksAreConsecutive(t *testing.T) {
	cs := []Candidate{cand('A', f64(1), 1), cand('B', nil, 0), cand('C', f64(3), 1), cand('D', f64(2), 1)}
	for i, a := range Elect(cs, []byte{'D'}) {
		if a.Rank != i {
			t.Fatalf("rank[%d] = %d", i, a.Rank)
		}
	}
}

func TestElectIgnoresInputOrderAndDoesNotMutate(t *testing.T) {
	base := []Candidate{
		cand('A', f64(6), 3), cand('B', f64(8.9), 2), cand('C', f64(8.9), 4),
		cand('D', nil, 0), cand('E', nil, 0), cand('F', f64(1), 9),
		failing(cand('G', f64(12), 5), MissLimit), failing(cand('H', nil, 0), MissLimit+2), failing(cand('I', f64(12), 5), 9),
	}
	base[4].ClaimedSNR = f32(7)
	want := order(Elect(base, []byte{'A'}))
	// Evidence first (healthy, then failing), then no evidence (healthy, then failing).
	if string(want) != "ACBFGIEDH" {
		t.Fatalf("order = %q", want)
	}
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 50; i++ {
		cs := append([]Candidate(nil), base...)
		rng.Shuffle(len(cs), func(i, j int) { cs[i], cs[j] = cs[j], cs[i] })
		snap := append([]Candidate(nil), cs...)
		got := order(Elect(cs, []byte{'A'}))
		if !bytes.Equal(got, want) {
			t.Fatalf("order %q != %q", got, want)
		}
		if !reflect.DeepEqual(cs, snap) {
			t.Fatal("input modified")
		}
	}
}

func failing(c Candidate, misses int) Candidate {
	c.Misses = misses
	return c
}

func TestElectFailingPollerDoesNotFlip(t *testing.T) {
	cs := []Candidate{failing(cand('A', f64(10), 1), 3), cand('B', f64(2), 1)}
	wantOrder(t, Elect(cs, []byte{'A'}), "BA")
	wantOrder(t, Elect(cs, []byte{'B'}), "BA")
}

func TestElectRecoveredPollerWinsByHysteresis(t *testing.T) {
	cs := []Candidate{cand('A', f64(10), 1), cand('B', f64(2), 1)}
	wantOrder(t, Elect(cs, []byte{'B'}), "AB")
}

func TestElectAllFailingRankedAmongThemselves(t *testing.T) {
	cs := []Candidate{failing(cand('B', f64(2), 1), 5), failing(cand('A', f64(10), 1), 3)}
	wantOrder(t, Elect(cs, nil), "AB")
	wantOrder(t, Elect(cs, []byte{'B'}), "AB")
}

func TestElectFailingNoEvidenceBelowHealthyNoEvidence(t *testing.T) {
	a := failing(cand('A', nil, 0), 3)
	a.ClaimedSNR = f32(20)
	b := cand('B', nil, 0)
	b.ClaimedSNR = f32(1)
	wantOrder(t, Elect([]Candidate{a, b}, nil), "BA")
}

func TestElectFailingNeverDisplacesHealthyCurrent(t *testing.T) {
	cs := []Candidate{cand('A', f64(1), 1), failing(cand('B', f64(30), 1), 3)}
	wantOrder(t, Elect(cs, []byte{'A'}), "AB")
}

func TestElectEvidenceBeforeHealth(t *testing.T) {
	// A has been heard through and missed three times; B has no evidence and claims no misses.
	a := failing(cand('A', f64(2), 4), MissLimit)
	b := cand('B', nil, 0)
	b.ClaimedSNR = f32(20)
	for _, cur := range [][]byte{nil, {'A'}, {'B'}} {
		wantOrder(t, Elect([]Candidate{b, a}, cur), "AB")
	}
}

func TestElectNoEvidenceCurrentLosesToFailingEvidence(t *testing.T) {
	cs := []Candidate{cand('A', nil, 0), failing(cand('B', f64(-10), 1), MissLimit+1)}
	wantOrder(t, Elect(cs, []byte{'A'}), "BA")
}

func TestElectSoleNoEvidenceStillPolls(t *testing.T) {
	wantOrder(t, Elect([]Candidate{cand('B', nil, 0)}, nil), "B")
	wantOrder(t, Elect([]Candidate{failing(cand('B', nil, 0), MissLimit)}, []byte{'B'}), "B")
}

func TestApprovedOnly(t *testing.T) {
	a, b := cand('A', f64(9), 3), cand('B', f64(1), 1)
	b.Approved = true
	in := []Candidate{a, b}
	if got := approvedOnly(in); len(got) != 1 || got[0].Observer[0] != 'B' {
		t.Fatalf("got %v", got)
	}
	if !reflect.DeepEqual(in, []Candidate{a, b}) {
		t.Fatal("input modified")
	}
}
