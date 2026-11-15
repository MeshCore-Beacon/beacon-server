// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

var policyNow = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

func policyCfg() config.ResolvedPolling {
	return config.ResolvedPolling{
		Enabled:      true,
		MQTTHosts:    []string{"mqtt.example.org"},
		DirectWindow: time.Hour,
		Grace:        30 * time.Minute,
		MaxRequests:  2,
	}
}

func key(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

func TestBuildPolicyAlwaysTelemetry(t *testing.T) {
	p := BuildPolicy(policyCfg(), RuntimeState{}, Caller{}, policyNow)
	if p.ReadingFormat != "telemetry" || p.DirectS != 3600 || p.OneHopS != 7200 || p.GraceS != 1800 || p.MaxRequests != 2 {
		t.Fatalf("unexpected policy %+v", p)
	}
	if p.ExcludedNodes == nil || p.NodeWindows == nil {
		t.Fatal("excluded_nodes and node_windows must not be nil")
	}
	b, _ := json.Marshal(p)
	s := string(b)
	for _, want := range []string{`"excluded_nodes":[]`, `"node_windows":{}`, `"paused_until":""`, `"min_plugin_version":""`, `"version_ok":true`} {
		if !strings.Contains(s, want) {
			t.Errorf("missing %s in %s", want, s)
		}
	}
}

func TestOneHopCapped(t *testing.T) {
	c := policyCfg()
	c.DirectWindow = 24 * time.Hour
	if p := BuildPolicy(c, RuntimeState{}, Caller{}, policyNow); p.OneHopS != 86400 {
		t.Fatalf("one_hop_s %d", p.OneHopS)
	}
}

func TestDisabledPolicy(t *testing.T) {
	p := DisabledPolicy()
	if p.ReadingFormat != "telemetry" || !p.Paused || !p.VersionOK {
		t.Fatalf("%+v", p)
	}
	if p.DirectS != 3600 || p.OneHopS != 7200 || p.GraceS != 1800 || p.MaxRequests != 2 {
		t.Fatalf("windows %+v", p)
	}
	b, _ := json.Marshal(p)
	for _, want := range []string{`"mqtt_hosts":[]`, `"excluded_nodes":[]`, `"node_windows":{}`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("missing %s in %s", want, b)
		}
	}
	if p.Version == "" || len(p.Version) > 15 {
		t.Fatalf("version %q", p.Version)
	}
}

func TestPolicyVersionChangesWithContent(t *testing.T) {
	a := BuildPolicy(policyCfg(), RuntimeState{}, Caller{}, policyNow)
	b := BuildPolicy(policyCfg(), RuntimeState{}, Caller{}, policyNow)
	if a.Version != b.Version || len(a.Version) != 12 {
		t.Fatalf("%q %q", a.Version, b.Version)
	}
	c := policyCfg()
	c.Grace = time.Hour
	if BuildPolicy(c, RuntimeState{}, Caller{}, policyNow).Version == a.Version {
		t.Fatal("grace change must change version")
	}
	c = policyCfg()
	c.MinClientVersion = "1.18.0"
	d := BuildPolicy(c, RuntimeState{}, Caller{Client: fwClient, ClientVersion: "1.0.0", Blocked: true}, policyNow)
	if !d.ObserverBlocked || d.VersionOK {
		t.Fatalf("%+v", d)
	}
	e := BuildPolicy(policyCfg(), RuntimeState{}, Caller{Blocked: true}, policyNow)
	if e.Version != a.Version {
		t.Fatal("caller fields must not change version")
	}
}

func TestPolicyPause(t *testing.T) {
	c := policyCfg()
	c.Paused = true
	if p := BuildPolicy(c, RuntimeState{}, Caller{}, policyNow); !p.Paused || p.PausedUntil != "" {
		t.Fatalf("%+v", p)
	}
	if p := BuildPolicy(policyCfg(), RuntimeState{Paused: true}, Caller{}, policyNow); !p.Paused || p.PausedUntil != "" {
		t.Fatalf("%+v", p)
	}
	past := policyNow.Add(-time.Minute)
	if p := BuildPolicy(policyCfg(), RuntimeState{Paused: true, PausedUntil: &past}, Caller{}, policyNow); p.Paused || p.PausedUntil != "" {
		t.Fatalf("%+v", p)
	}
	fut := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	if p := BuildPolicy(policyCfg(), RuntimeState{Paused: true, PausedUntil: &fut}, Caller{}, policyNow); !p.Paused || p.PausedUntil != "2026-11-14T12:00:00Z" {
		t.Fatalf("%+v", p)
	}
	// An end time without the paused flag is ignored.
	if p := BuildPolicy(policyCfg(), RuntimeState{PausedUntil: &fut}, Caller{}, policyNow); p.Paused {
		t.Fatalf("%+v", p)
	}
}

func TestPolicyVersionGate(t *testing.T) {
	ok := func(min, client, ver string) bool {
		c := policyCfg()
		c.MinClientVersion = min
		return BuildPolicy(c, RuntimeState{}, Caller{Client: client, ClientVersion: ver}, policyNow).VersionOK
	}
	if !ok("", fwClient, "") || !ok("", "other", "0.0.1") {
		t.Fatal("no minimum must pass")
	}
	cases := []struct {
		ver  string
		want bool
	}{{"1.17.9", false}, {"1.18.0", true}, {"v1.18.1-dev", true}, {"", false}, {"junk", false}, {"1.2", false}}
	for _, tc := range cases {
		if got := ok("1.18.0", fwClient, tc.ver); got != tc.want {
			t.Errorf("%q: got %v", tc.ver, got)
		}
	}
	if !ok("1.18.0", "other", "0.1.0") || !ok("1.18.0", "", "") {
		t.Fatal("other clients are not gated")
	}
	// Config rejects a bad minimum at load; should one get through, it no longer opens the gate.
	if ok("junk", fwClient, "1.18.0") {
		t.Fatal("bad configured minimum passed")
	}
}

func TestPolicyBlockedCaller(t *testing.T) {
	if !BuildPolicy(policyCfg(), RuntimeState{}, Caller{Blocked: true}, policyNow).ObserverBlocked {
		t.Fatal("blocked")
	}
}

func TestPolicyExcludedMergesConfigAndRuntime(t *testing.T) {
	c := policyCfg()
	c.ExcludedNodes = [][]byte{key(0xAB), key(0x01)}
	rt := RuntimeState{Excluded: [][]byte{key(0xab), key(0x02)}}
	p := BuildPolicy(c, rt, Caller{}, policyNow)
	want := []string{strings.Repeat("ab", 32), strings.Repeat("01", 32), strings.Repeat("02", 32)}
	if len(p.ExcludedNodes) != 3 {
		t.Fatalf("%v", p.ExcludedNodes)
	}
	for i := range want {
		if p.ExcludedNodes[i] != want[i] {
			t.Errorf("%d: %s", i, p.ExcludedNodes[i])
		}
	}
	var many [][]byte
	for i := 0; i < 20; i++ {
		many = append(many, key(byte(i)))
	}
	p = BuildPolicy(c, RuntimeState{Excluded: many}, Caller{}, policyNow)
	if len(p.ExcludedNodes) != 16 {
		t.Fatalf("len %d", len(p.ExcludedNodes))
	}
}

func TestBuildOwners(t *testing.T) {
	ts := time.Date(2026, 11, 14, 9, 30, 15, 900, time.FixedZone("x", 3600))
	rows := []OwnerRow{
		{Target: key(0xAA), Rank: 0, LatestTS: &ts},
		{Target: key(0xBB), Rank: 2},
		{Target: key(0xCC), Rank: -1},
		{Target: key(0xDD), Rank: 0, Excluded: true},
	}
	a := BuildOwners(rows, DisabledPolicy())
	e := a.Nodes[strings.Repeat("aa", 32)]
	if e.Role != "polling" || !e.Mine || e.StandbyRank != 1 || e.OwnerHops != 0 || e.LatestTS == nil || *e.LatestTS != "2026-11-14T08:30:15Z" {
		t.Fatalf("%+v", e)
	}
	e = a.Nodes[strings.Repeat("bb", 32)]
	if e.Role != "standby" || e.Mine || e.StandbyRank != 2 || e.LatestTS != nil {
		t.Fatalf("%+v", e)
	}
	e = a.Nodes[strings.Repeat("cc", 32)]
	if e.Role != "polling" || !e.Mine || e.StandbyRank != 1 {
		t.Fatalf("%+v", e)
	}
	e = a.Nodes[strings.Repeat("dd", 32)]
	if e.Role != "excluded" || !e.Excluded || e.Mine {
		t.Fatalf("%+v", e)
	}
	b, _ := json.Marshal(a)
	if !strings.Contains(string(b), `"latest_ts":null`) {
		t.Fatalf("%s", b)
	}
	if empty, _ := json.Marshal(BuildOwners(nil, DisabledPolicy())); !strings.Contains(string(empty), `"nodes":{}`) {
		t.Fatalf("%s", empty)
	}
}

func TestOwnersAnswerFitsFirmwareCap(t *testing.T) {
	c := policyCfg()
	c.MQTTHosts = nil
	for i := 0; i < 4; i++ {
		c.MQTTHosts = append(c.MQTTHosts, strings.Repeat(string(rune('a'+i)), 63))
	}
	var ex [][]byte
	for i := 0; i < 16; i++ {
		ex = append(ex, key(byte(i+1)))
	}
	c.ExcludedNodes = ex
	var rows []OwnerRow
	ts := policyNow
	for i := 0; i < 8; i++ {
		rows = append(rows, OwnerRow{Target: key(byte(0x80 + i)), Rank: 3, LatestTS: &ts})
	}
	b, err := json.Marshal(BuildOwners(rows, BuildPolicy(c, RuntimeState{}, Caller{}, policyNow)))
	if err != nil {
		t.Fatal(err)
	}
	if len(b) >= 6144 {
		t.Fatalf("answer is %d bytes", len(b))
	}
}

func TestOfferSeatsCountsWithdrawnTargetsAsFree(t *testing.T) {
	key := func(b byte) []byte { return []byte{b} }
	rows := []OwnerRow{
		{Target: key(1), Rank: 0, Requested: true, ClaimedRequested: true}, // held and still assigned
		{Target: key(2), Rank: -1, ClaimedRequested: true},                 // held, withdrawn: its seat is free
		{Target: key(3), Rank: 1, Requested: true},                         // new standby
		{Target: key(4), Rank: 0, Requested: true},                         // new poller
		{Target: key(5), Rank: 0},                                          // local
	}
	for i := range rows {
		rows[i].AcceptsRequests, rows[i].TargetRequested = true, true
	}
	OfferSeats(rows)
	got := []bool{rows[0].PWOK, rows[1].PWOK, rows[2].PWOK, rows[3].PWOK, rows[4].PWOK}
	if want := []bool{true, false, false, true, false}; !slices.Equal(got, want) {
		t.Fatalf("zero free slots: %v, want %v", got, want)
	}
	for i := range rows {
		rows[i].RequestSlots = 1
	}
	if OfferSeats(rows); !rows[2].PWOK || !rows[3].PWOK {
		t.Fatalf("one free slot and one withdrawn: %+v", rows)
	}
	for i := range rows {
		rows[i].AcceptsRequests = false
	}
	if OfferSeats(rows); rows[0].PWOK || rows[3].PWOK {
		t.Fatal("a claim that takes no requests is offered none")
	}
	// A held target whose requests are gone is withdrawn, and frees its seat for the new poller.
	for i := range rows {
		rows[i].AcceptsRequests, rows[i].RequestSlots = true, 0
	}
	rows[0].TargetRequested = false
	rows[1].ClaimedRequested = false
	if OfferSeats(rows); rows[0].PWOK || !rows[3].PWOK || rows[2].PWOK {
		t.Fatalf("withdrawn target kept its seat: %+v", rows)
	}
}
