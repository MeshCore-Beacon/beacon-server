// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func readFile(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func mustClaim(t *testing.T, name string) Claim {
	t.Helper()
	c, err := ParseClaim(readFile(t, name))
	if err != nil {
		t.Fatalf("ParseClaim %s: %v", name, err)
	}
	return c
}

func mustReadings(t *testing.T, name string) []Reading {
	t.Helper()
	r, err := ParseReadings(readFile(t, name))
	if err != nil {
		t.Fatalf("ParseReadings %s: %v", name, err)
	}
	return r
}

func eq[T comparable](t *testing.T, what string, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

func near(t *testing.T, what string, got *float64, want float64) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s is nil, want %v", what, want)
	}
	if d := *got - want; d > 0.001 || d < -0.001 {
		t.Errorf("%s = %v, want %v", what, *got, want)
	}
}

func TestParseClaimExtended(t *testing.T) {
	c := mustClaim(t, "claim_extended.json")
	eq(t, "AcceptsRequests", c.AcceptsRequests, true)
	eq(t, "RequestSlots", c.RequestSlots, 3)
	eq(t, "Origin", c.Origin, "Cadham Village")
	eq(t, "Client", c.Client, "meshcore.observer_fw/example")
	eq(t, "PolicyVersion", c.PolicyVersion, "a1b2c3d4")
	eq(t, "Community", c.Community, true)
	eq(t, "Observer len", len(c.Observer), 32)
	eq(t, "Timestamp", c.Timestamp.UTC().Format(time.RFC3339), "2023-11-14T22:13:20Z")
	eq(t, "nodes", len(c.Nodes), 1)
	n := c.Nodes[0]
	eq(t, "KeyMode", n.KeyMode, "own")
	eq(t, "Requested", n.Requested, true)
	eq(t, "State", n.State, "ok")
	eq(t, "Misses", n.Misses, 2)
	eq(t, "Target len", len(n.Target), 32)
	if n.ClaimedSNR == nil || *n.ClaimedSNR < 6.499 || *n.ClaimedSNR > 6.501 {
		t.Errorf("ClaimedSNR = %v, want 6.5", n.ClaimedSNR)
	}
	eq(t, "LastOK", n.LastOK.UTC().Format(time.RFC3339), "2023-11-14T22:12:30Z")
	eq(t, "LastAttempt", n.LastAttempt.UTC().Format(time.RFC3339), "2023-11-14T22:11:40Z")
}

func TestParseClaimEmptyNodes(t *testing.T) {
	c := mustClaim(t, "claim_extended_empty.json")
	eq(t, "nodes", len(c.Nodes), 0)
	eq(t, "RequestSlots", c.RequestSlots, 8)
	eq(t, "AcceptsRequests", c.AcceptsRequests, true)
}

func TestParseClaimPlainV2HasNoState(t *testing.T) {
	c := mustClaim(t, "claim_plain_v2.json")
	eq(t, "AcceptsRequests", c.AcceptsRequests, false)
	eq(t, "RequestSlots", c.RequestSlots, 0)
	eq(t, "nodes", len(c.Nodes), 1)
	n := c.Nodes[0]
	eq(t, "State", n.State, "")
	eq(t, "KeyMode", n.KeyMode, "shared")
	eq(t, "Requested", n.Requested, false)
	if n.LastOK != nil || n.LastAttempt != nil {
		t.Errorf("last_ok/last_attempt should be nil")
	}
}

func TestParseClaimRejects(t *testing.T) {
	good := string(readFile(t, "claim_extended.json"))
	obs := "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F"
	node := strings.Repeat("ab", 32)
	cases := map[string]string{
		"origin_id short":   strings.Replace(good, obs, obs[:62], 1),
		"origin_id not hex": strings.Replace(good, obs, strings.Repeat("zz", 32), 1),
		"node_id short":     strings.Replace(good, node, node[:62], 1),
		"timestamp no zone": strings.Replace(good, `"2023-11-14T22:13:20Z"`, `"2023-11-14T22:13:20"`, 1),
		"v is 1":            strings.Replace(good, `"v": 2`, `"v": 1`, 1),
		"reply_hops 2":      strings.Replace(good, `"reply_hops": 0`, `"reply_hops": 2`, 1),
		"not json":          "{",
	}
	for name, doc := range cases {
		if _, err := ParseClaim([]byte(doc)); err == nil {
			t.Errorf("%s: want error", name)
		}
	}

	nodes := func(n int) []byte {
		one := `{"node_id":"` + node + `","key":"own"}`
		list := strings.TrimSuffix(strings.Repeat(one+",", n), ",")
		return []byte(`{"v":2,"origin_id":"` + obs + `","timestamp":"2023-11-14T22:13:20Z","nodes":[` + list + `]}`)
	}
	if _, err := ParseClaim(nodes(9)); err != nil {
		t.Errorf("9 nodes should be accepted: %v", err)
	}
	if _, err := ParseClaim(nodes(32)); err != nil {
		t.Errorf("32 nodes should be accepted: %v", err)
	}
	if _, err := ParseClaim(nodes(33)); err == nil {
		t.Error("33 nodes should be rejected")
	}
	big := append([]byte(good), bytes.Repeat([]byte(" "), 16384)...)
	if _, err := ParseClaim(big); err == nil {
		t.Error("oversized claim should be rejected")
	}
}

func TestParseClaimAcceptsLowerHexAndIgnoresVia(t *testing.T) {
	doc := strings.Replace(string(readFile(t, "claim_plain_v2.json")),
		"000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F",
		"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", 1)
	doc = strings.Replace(doc, `"via": null`, `"via": "`+strings.Repeat("ef", 32)+`"`, 1)
	doc = strings.Replace(doc, `"reply_hops": 0`, `"reply_hops": 1`, 1)
	c, err := ParseClaim([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "Observer", hex.EncodeToString(c.Observer), "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
}

func TestParseClaimCleansStrings(t *testing.T) {
	doc := strings.Replace(string(readFile(t, "claim_extended.json")), "Cadham Village", `Cad\u0000ham`, 1)
	c, err := ParseClaim([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "Origin", c.Origin, "Cadham")
}

func TestParseReadingV2Status(t *testing.T) {
	r := mustReadings(t, "v2_status.json")[0]
	eq(t, "Version", r.Version, 2)
	eq(t, "Method", r.Method, "status")
	eq(t, "Client", r.Client, "meshcore.observer_fw/example")
	eq(t, "Observer len", len(r.Observer), 32)
	eq(t, "Target len", len(r.Target), 32)
	eq(t, "SampledAt", r.SampledAt.UTC().Format(time.RFC3339), "2023-11-14T22:13:20Z")
	eq(t, "BatteryMV", *r.BatteryMV, 4038)
	near(t, "PollSNR", r.PollSNR, 6.5)
	eq(t, "UptimeSecs", *r.Stats.UptimeSecs, int64(788089))
	eq(t, "NoiseFloor", *r.Stats.NoiseFloor, -116)
	eq(t, "LastRSSI", *r.Stats.LastRSSI, -114)
	near(t, "LastSNR", r.Stats.LastSNR, 1.25)
	eq(t, "TxQueue", *r.Stats.TxQueue, 0)
	eq(t, "PacketsRecv", *r.Stats.PacketsRecv, int64(81234))
	eq(t, "PacketsSent", *r.Stats.PacketsSent, int64(20276))
	eq(t, "RecvFlood", *r.Stats.RecvFlood, int64(76198))
	eq(t, "RecvDirect", *r.Stats.RecvDirect, int64(5012))
	eq(t, "SentFlood", *r.Stats.SentFlood, int64(19211))
	eq(t, "SentDirect", *r.Stats.SentDirect, int64(1065))
	eq(t, "TxAirSecs", *r.Stats.TxAirSecs, int64(18301))
	eq(t, "RxAirSecs", *r.Stats.RxAirSecs, int64(73835))
	eq(t, "FloodDups", *r.Stats.FloodDups, 56298)
	eq(t, "DirectDups", *r.Stats.DirectDups, 36)
	eq(t, "RecvErrors", *r.Stats.RecvErrors, int64(28074))
	eq(t, "ErrEvents", *r.Stats.ErrEvents, 2)
	eq(t, "Tag", *r.Tag, uint32(1794484800))
	eq(t, "PacketHash len", len(r.PacketHash), 8)
	if r.LPP != nil {
		t.Errorf("LPP = %x, want nil", r.LPP)
	}
}

func TestParseReadingV2Telemetry(t *testing.T) {
	r := mustReadings(t, "v2_telemetry.json")[0]
	eq(t, "Method", r.Method, "telemetry")
	eq(t, "BatteryMV", *r.BatteryMV, 3940)
	near(t, "TemperatureC", r.TemperatureC, 16.5)
	eq(t, "LPP", hex.EncodeToString(r.LPP), "0174018a016700a5")
}

func TestParseReadingNoVoltageUnknownTail(t *testing.T) {
	r := mustReadings(t, "v2_telemetry_no_voltage.json")[0]
	if r.BatteryMV != nil {
		t.Errorf("BatteryMV = %d, want nil", *r.BatteryMV)
	}
	if r.TemperatureC != nil {
		t.Errorf("TemperatureC = %v, want nil", *r.TemperatureC)
	}
	eq(t, "LPP", hex.EncodeToString(r.LPP), "0267006401fa")
}

func TestParseReadingJSONWinsOverLPP(t *testing.T) {
	doc := strings.Replace(string(readFile(t, "v2_telemetry.json")), `"battery_mv":3940`, `"battery_mv":3800`, 1)
	rs, err := ParseReadings([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "BatteryMV", *rs[0].BatteryMV, 3800)
}

func TestParseReadingLPPFillsWhenJSONOmits(t *testing.T) {
	doc := strings.Replace(string(readFile(t, "v2_telemetry.json")), `"battery_mv":3940,`, "", 1)
	doc = strings.Replace(doc, `"temperature_c":16.5,`, "", 1)
	rs, err := ParseReadings([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "BatteryMV", *rs[0].BatteryMV, 3940)
	near(t, "TemperatureC", rs[0].TemperatureC, 16.5)
}

func TestParseReadingZeroBatteryIsAbsent(t *testing.T) {
	doc := strings.Replace(string(readFile(t, "v1_telemetry.json")), `"battery_mv":3940`, `"battery_mv":0`, 1)
	rs, err := ParseReadings([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if rs[0].BatteryMV != nil {
		t.Errorf("BatteryMV = %d, want nil", *rs[0].BatteryMV)
	}
}

func TestParseReadingStatsPartial(t *testing.T) {
	doc := strings.Replace(string(readFile(t, "v2_status.json")), "\"stats\":{", `"stats":{}, "x":{`, 1)
	rs, err := ParseReadings([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if rs[0].Stats != (Stats{}) {
		t.Errorf("Stats = %+v, want zero value", rs[0].Stats)
	}
}

func TestParseReadingStatsSubset(t *testing.T) {
	doc := strings.Replace(string(readFile(t, "v2_status.json")), `"stats":{`, `"stats":{"tx_queue":3,"nope":1}, "x":{`, 1)
	rs, err := ParseReadings([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	s := rs[0].Stats
	eq(t, "TxQueue", *s.TxQueue, 3)
	if s.UptimeSecs != nil || s.NoiseFloor != nil || s.LastSNR != nil {
		t.Errorf("absent stats should be nil: %+v", s)
	}
}

func TestParseReadingV1(t *testing.T) {
	r := mustReadings(t, "v1_telemetry.json")[0]
	eq(t, "Version", r.Version, 1)
	if r.Tag != nil || r.PacketHash != nil {
		t.Errorf("Tag and PacketHash should be nil")
	}
	eq(t, "BatteryMV", *r.BatteryMV, 3940)
	near(t, "TemperatureC", r.TemperatureC, 16.5)
}

func TestParseReadingRejects(t *testing.T) {
	good := string(readFile(t, "v2_status.json"))
	node := strings.Repeat("22", 32)
	obs := "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F"
	cases := map[string]string{
		"method history":     strings.Replace(good, `"method":"status"`, `"method":"history"`, 1),
		"sampled_at absent":  strings.Replace(good, `"sampled_at":"2023-11-14T22:13:20Z",`, "", 1),
		"sampled_at no zone": strings.Replace(good, `"sampled_at":"2023-11-14T22:13:20Z"`, `"sampled_at":"2023-11-14T22:13:20"`, 1),
		"node_id short":      strings.Replace(good, node, node[:62], 1),
		"origin_id bad":      strings.Replace(good, obs, obs[:60], 1),
		"version 3":          strings.Replace(good, `"v":2`, `"v":3`, 1),
		"zero readings":      `{"v":2,"origin_id":"` + obs + `","readings":[]}`,
		"no readings":        `{"v":2,"origin_id":"` + obs + `"}`,
		"lpp bad hex":        strings.Replace(good, `"tag"`, `"lpp":"zz","tag"`, 1),
		"not json":           "[",
	}
	for name, doc := range cases {
		if _, err := ParseReadings([]byte(doc)); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

func TestParseReadingLPPTooLong(t *testing.T) {
	good := string(readFile(t, "v2_telemetry.json"))
	doc := strings.Replace(good, "0174018a016700a5", strings.Repeat("01", 161), 1)
	if _, err := ParseReadings([]byte(doc)); err == nil {
		t.Error("161 byte lpp should be rejected")
	}
	doc = strings.Replace(good, "0174018a016700a5", strings.Repeat("01", 160), 1)
	if _, err := ParseReadings([]byte(doc)); err != nil {
		t.Errorf("160 byte lpp should be accepted: %v", err)
	}
}

func TestDecodeLPP(t *testing.T) {
	cases := []struct {
		name string
		hex  string
		mv   *int
		c    *float64
	}{
		{"voltage and temperature", "0174018a016700a5", ip(3940), fp(16.5)},
		{"channel 2 ignored, unknown tail", "0267006401fa", nil, nil},
		{"zero temperature", "01670000", nil, fp(0)},
		{"lower bound -600", "0167fda8", nil, fp(-60.0)},
		{"below lower bound -601", "0167fda7", nil, nil},
		{"upper bound 1250", "016704e2", nil, fp(125.0)},
		{"above upper bound 1251", "016704e3", nil, nil},
		{"empty", "", nil, nil},
		{"stops at channel 0", "000001740" + "18a", nil, nil},
		{"truncated voltage", "017401", nil, nil},
		{"skips other record then reads", "0268006401" + "74018a", ip(3940), nil},
		{"first temperature wins even if out of range", "0167fd00" + "016700a5", nil, nil},
		{"unknown type stops walk", "01fa00" + "01" + "74018a", nil, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := hex.DecodeString(tc.hex)
			if err != nil {
				t.Fatal(err)
			}
			mv, c := DecodeLPP(b)
			if (mv == nil) != (tc.mv == nil) || (mv != nil && *mv != *tc.mv) {
				t.Errorf("mv = %v, want %v", deref(mv), deref(tc.mv))
			}
			if (c == nil) != (tc.c == nil) || (c != nil && (*c-*tc.c > 0.001 || *tc.c-*c > 0.001)) {
				t.Errorf("c = %v, want %v", c, tc.c)
			}
		})
	}
}

func ip(i int) *int         { return &i }
func fp(f float64) *float64 { return &f }
func deref(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}

func TestParseClaimUnknownStateAndKeyKept(t *testing.T) {
	doc := strings.Replace(string(readFile(t, "claim_extended.json")), `"state": "ok"`, `"state": "paused_by_owner"`, 1)
	doc = strings.Replace(doc, `"key": "own"`, `"key": "delegated"`, 1)
	c, err := ParseClaim([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "State", c.Nodes[0].State, "paused_by_owner")
	eq(t, "KeyMode", c.Nodes[0].KeyMode, "delegated")
}

func TestParseClaimLongStateCut(t *testing.T) {
	long := strings.Repeat("a", 40)
	doc := strings.Replace(string(readFile(t, "claim_extended.json")), `"state": "ok"`, `"state": "`+long+`"`, 1)
	c, err := ParseClaim([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "State", c.Nodes[0].State, long[:32])
}

func TestParseClaimClampsSlots(t *testing.T) {
	for in, want := range map[string]int{"9": 8, "1000": 8, "-2": 0} {
		doc := strings.Replace(string(readFile(t, "claim_extended.json")), `"request_slots": 3`, `"request_slots": `+in, 1)
		c, err := ParseClaim([]byte(doc))
		if err != nil {
			t.Fatal(err)
		}
		eq(t, "RequestSlots for "+in, c.RequestSlots, want)
	}
}

func TestParseClaimClampsMisses(t *testing.T) {
	for in, want := range map[string]int{"-1": 0, "256": 255, "100000": 255, "7": 7} {
		doc := strings.Replace(string(readFile(t, "claim_extended.json")), `"misses": 2`, `"misses": `+in, 1)
		c, err := ParseClaim([]byte(doc))
		if err != nil {
			t.Fatal(err)
		}
		eq(t, "Misses for "+in, c.Nodes[0].Misses, want)
	}
}

func TestParseReadingsCapsEntries(t *testing.T) {
	obs := "000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F"
	one := `{"node_id":"` + strings.Repeat("22", 32) + `","method":"status","sampled_at":"2023-11-14T22:13:20Z"}`
	doc := func(n int) []byte {
		return []byte(`{"v":2,"origin_id":"` + obs + `","readings":[` + strings.TrimSuffix(strings.Repeat(one+",", n), ",") + `]}`)
	}
	if _, err := ParseReadings(doc(8)); err != nil {
		t.Errorf("8 readings should be accepted: %v", err)
	}
	if _, err := ParseReadings(doc(9)); err == nil {
		t.Error("9 readings should be rejected")
	}
}

func TestParseReadingOutOfRangeIntsAreAbsent(t *testing.T) {
	const (
		i32Over  = "2147483648"
		i32Under = "-2147483649"
		i64Over  = "9223372036854775808"
		i64Under = "-9223372036854775809"
	)
	stats := `"stats":{"uptime_secs":` + i64Over + `,"packets_recv":9223372036854775807,"recv_errors":` + i64Under +
		`,"noise_floor":` + i32Over + `,"last_rssi":2147483647,"tx_queue":` + i32Under + `,"err_events":` + i64Over + `},"x":{`
	doc := strings.Replace(string(readFile(t, "v2_status.json")), `"stats":{`, stats, 1)
	doc = strings.Replace(doc, `"battery_mv":4038`, `"battery_mv":`+i64Over, 1)
	rs, err := ParseReadings([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	r := rs[0]
	s := r.Stats
	if s.UptimeSecs != nil || s.RecvErrors != nil || s.NoiseFloor != nil || s.TxQueue != nil || s.ErrEvents != nil || r.BatteryMV != nil {
		t.Errorf("out of range values should be absent: %+v battery=%v", s, r.BatteryMV)
	}
	eq(t, "PacketsRecv", *s.PacketsRecv, int64(9223372036854775807))
	eq(t, "LastRSSI", *s.LastRSSI, 2147483647)
}

func TestParseClaimHugeIntsAreAbsent(t *testing.T) {
	doc := strings.Replace(string(readFile(t, "claim_extended.json")), `"misses": 2`, `"misses": 9223372036854775808`, 1)
	doc = strings.Replace(doc, `"request_slots": 3`, `"request_slots": -9223372036854775809`, 1)
	c, err := ParseClaim([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "Misses", c.Nodes[0].Misses, 0)
	eq(t, "RequestSlots", c.RequestSlots, 0)
	doc = strings.Replace(string(readFile(t, "claim_extended.json")), `"misses": 2`, `"misses": 2147483648`, 1)
	if c, err = ParseClaim([]byte(doc)); err != nil {
		t.Fatal(err)
	}
	eq(t, "Misses for 2^31", c.Nodes[0].Misses, 255)
}

func TestParseReadingPacketHashMustBeEightBytes(t *testing.T) {
	good := string(readFile(t, "v2_status.json"))
	for name, h := range map[string]string{
		"7000 bytes": strings.Repeat("ab", 7000),
		"9 bytes":    strings.Repeat("ab", 9),
		"7 bytes":    strings.Repeat("ab", 7),
		"not hex":    "xyz",
		"odd length": "0123456789ABCDE",
	} {
		rs, err := ParseReadings([]byte(strings.Replace(good, "0123456789ABCDEF", h, 1)))
		if err != nil {
			t.Errorf("%s: reading rejected: %v", name, err)
			continue
		}
		if rs[0].PacketHash != nil {
			t.Errorf("%s: packet hash kept (%d bytes)", name, len(rs[0].PacketHash))
		}
	}
	rs, err := ParseReadings([]byte(good))
	if err != nil || len(rs[0].PacketHash) != 8 {
		t.Fatal("8-byte hash lost", err)
	}
}
