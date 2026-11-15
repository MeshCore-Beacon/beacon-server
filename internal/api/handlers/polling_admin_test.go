// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

type pollAdminStub struct {
	paused    bool
	until     *time.Time
	excluded  [][]byte
	observers []api.PollingObserver
	err       error
	writes    int
	obsKey    []byte
	obs       api.PollingObserverRequest
	targets   []api.PollingTarget
	tAfter    time.Time
	tLimit    int
	stops     []api.PollingAdminStop
	blocked   []string
	found     bool
	stopKeys  [][]byte
}

func (s *pollAdminStub) PollingTargetsAdmin(_ context.Context, after time.Time, limit int) ([]api.PollingTarget, error) {
	s.tAfter, s.tLimit = after, limit
	return s.targets, s.err
}

func (s *pollAdminStub) PollingState(context.Context) (bool, *time.Time, [][]byte, error) {
	return s.paused, s.until, s.excluded, s.err
}
func (s *pollAdminStub) SetPollingState(_ context.Context, p bool, u *time.Time, e [][]byte) error {
	s.writes++
	s.paused, s.until, s.excluded = p, u, e
	return s.err
}
func (s *pollAdminStub) ListPollingObservers(context.Context) ([]api.PollingObserver, error) {
	return s.observers, s.err
}
func (s *pollAdminStub) SetPollingObserver(_ context.Context, k []byte, b, a bool, n string) error {
	s.writes++
	s.obsKey, s.obs = k, api.PollingObserverRequest{Blocked: b, Approved: a, Note: n}
	return s.err
}

func (s *pollAdminStub) ListPollingAdminStops(context.Context) ([]api.PollingAdminStop, error) {
	return s.stops, s.err
}
func (s *pollAdminStub) ListNoPasswordBlocked(context.Context) ([]string, error) {
	return s.blocked, s.err
}
func (s *pollAdminStub) DeletePollingAdminStop(_ context.Context, t, o []byte) (bool, error) {
	s.writes++
	s.stopKeys = [][]byte{t, o}
	return s.found, s.err
}
func (s *pollAdminStub) ClearNoPasswordBlocked(_ context.Context, t []byte) (bool, error) {
	s.writes++
	s.stopKeys = [][]byte{t}
	return s.found, s.err
}

func pollAdminDo(h http.Handler, method, path, body, ct string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if ct != "" {
		req.Header.Set("Content-Type", ct)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func hexKey(c string) string { return strings.Repeat(c, 64) }

func TestPollingAdminGet(t *testing.T) {
	until := time.Now().Add(time.Hour).Truncate(time.Millisecond)
	st := &pollAdminStub{paused: true, until: &until, excluded: [][]byte{make([]byte, 32)},
		observers: []api.PollingObserver{{PublicKey: hexKey("a"), Blocked: true, Note: "n"}}}
	rec := pollAdminDo(PollingAdminRouter(st, PollingAdminConfig{}), "GET", "/", "", "")
	if rec.Code != 200 {
		t.Fatal(rec.Code, rec.Body.String())
	}
	var got api.PollingAdminState
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !got.Paused || got.PausedUntil == nil || *got.PausedUntil != until.UnixMilli() ||
		len(got.ExcludedNodes) != 1 || got.ExcludedNodes[0] != hexKey("0") || len(got.Observers) != 1 || !got.Observers[0].Blocked {
		t.Fatalf("%+v", got)
	}
	// Empty state encodes null pausedUntil and empty arrays, not null arrays.
	rec = pollAdminDo(PollingAdminRouter(&pollAdminStub{}, PollingAdminConfig{}), "GET", "/", "", "")
	if !strings.Contains(rec.Body.String(), `"pausedUntil":null`) || !strings.Contains(rec.Body.String(), `"excludedNodes":[]`) || !strings.Contains(rec.Body.String(), `"observers":[]`) {
		t.Fatal(rec.Body.String())
	}
	if rec := pollAdminDo(PollingAdminRouter(&pollAdminStub{err: errors.New("x")}, PollingAdminConfig{}), "GET", "/", "", ""); rec.Code != 500 {
		t.Fatal(rec.Code)
	}
}

func TestPollingAdminPutState(t *testing.T) {
	const ct = "application/json"
	st := &pollAdminStub{}
	h := PollingAdminRouter(st, PollingAdminConfig{})
	future := time.Now().Add(time.Hour).UnixMilli()
	nodes := func(n int) string {
		var out []string
		for i := 0; i < n; i++ {
			out = append(out, fmt.Sprintf(`"%064x"`, i+1))
		}
		return strings.Join(out, ",")
	}
	for name, c := range map[string]struct {
		body, ct string
		want     int
	}{
		"17 nodes":      {`{"paused":false,"pausedUntil":null,"excludedNodes":[` + nodes(17) + `]}`, ct, 400},
		"bad hex":       {`{"paused":false,"pausedUntil":null,"excludedNodes":["zz"]}`, ct, 400},
		"unknown field": {`{"paused":false,"pausedUntil":null,"excludedNodes":[],"x":1}`, ct, 400},
		"past pause":    {`{"paused":true,"pausedUntil":1000,"excludedNodes":[]}`, ct, 400},
		"trailing":      {`{"paused":false,"excludedNodes":[]} {}`, ct, 400},
		"too big":       {`{"paused":false,"excludedNodes":[],"pad":"` + strings.Repeat("a", 5000) + `"}`, ct, 400},
		"wrong type":    {`{}`, "text/plain", 415},
	} {
		if rec := pollAdminDo(h, "PUT", "/state", c.body, c.ct); rec.Code != c.want {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
	if st.writes != 0 {
		t.Fatal("a rejected request wrote")
	}
	body := `{"paused":true,"pausedUntil":` + itoa(future) + `,"excludedNodes":["` + strings.ToUpper(hexKey("c")) + `"]}`
	rec := pollAdminDo(h, "PUT", "/state", body, ct)
	if rec.Code != 200 || st.writes != 1 || !st.paused || st.until == nil || st.until.UnixMilli() != future || len(st.excluded) != 1 || st.excluded[0][0] != 0xcc {
		t.Fatal(rec.Code, rec.Body.String(), st)
	}
	if !strings.Contains(rec.Body.String(), hexKey("c")) {
		t.Fatal("response should show lowercase hex", rec.Body.String())
	}
}

func itoa(n int64) string { b, _ := json.Marshal(n); return string(b) }

func TestPollingAdminPutObserver(t *testing.T) {
	const ct = "application/json"
	st := &pollAdminStub{}
	h := PollingAdminRouter(st, PollingAdminConfig{})
	for _, k := range []string{"abc", hexKey("g"), hexKey("a") + "0"} {
		if rec := pollAdminDo(h, "PUT", "/observers/"+k, `{"blocked":true}`, ct); rec.Code != 400 {
			t.Errorf("%q: %d", k, rec.Code)
		}
	}
	if rec := pollAdminDo(h, "PUT", "/observers/"+hexKey("a"), `{"note":"`+strings.Repeat("é", 201)+`"}`, ct); rec.Code != 400 {
		t.Error("201 char note", rec.Code)
	}
	if rec := pollAdminDo(h, "PUT", "/observers/"+hexKey("a"), `{"blocked":true,"extra":1}`, ct); rec.Code != 400 {
		t.Error("unknown field", rec.Code)
	}
	if st.writes != 0 {
		t.Fatal("a rejected request wrote")
	}
	rec := pollAdminDo(h, "PUT", "/observers/"+strings.ToUpper(hexKey("a")), `{"blocked":true,"approved":true,"note":"`+strings.Repeat("é", 200)+`"}`, ct)
	if rec.Code != 200 || st.writes != 1 || st.obsKey[0] != 0xaa || len(st.obsKey) != 32 || !st.obs.Blocked || !st.obs.Approved {
		t.Fatal(rec.Code, rec.Body.String())
	}
}

func TestPollingAdminNilStore(t *testing.T) {
	for _, c := range [][2]string{{"GET", "/"}, {"GET", "/targets"}, {"PUT", "/state"}, {"PUT", "/observers/" + hexKey("a")}} {
		if rec := pollAdminDo(PollingAdminRouter(nil, PollingAdminConfig{}), c[0], c[1], `{}`, "application/json"); rec.Code != 503 {
			t.Errorf("%v: %d", c, rec.Code)
		}
	}
}

func TestPollingAdminEffectivePause(t *testing.T) {
	past := time.Now().Add(-time.Minute)
	for name, c := range map[string]struct {
		st         *pollAdminStub
		cfg        bool
		paused     bool
		cfgPaused  bool
		untilShown bool
	}{
		"pause lapsed":   {&pollAdminStub{paused: true, until: &past}, false, false, false, true},
		"open pause":     {&pollAdminStub{paused: true}, false, true, false, false},
		"config pause":   {&pollAdminStub{}, true, false, true, false},
		"nothing paused": {&pollAdminStub{}, false, false, false, false},
	} {
		rec := pollAdminDo(PollingAdminRouter(c.st, PollingAdminConfig{Paused: c.cfg}), "GET", "/", "", "")
		var got api.PollingAdminState
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || rec.Code != 200 {
			t.Fatal(name, rec.Code, err)
		}
		if got.Paused != c.paused || got.ConfigPaused != c.cfgPaused || (got.PausedUntil != nil) != c.untilShown {
			t.Errorf("%s: %s", name, rec.Body.String())
		}
	}
}

func TestPollingAdminPutStateDedupesNodes(t *testing.T) {
	st := &pollAdminStub{}
	var nodes []string
	for range 20 {
		nodes = append(nodes, `"`+hexKey("b")+`"`, `"`+strings.ToUpper(hexKey("b"))+`"`)
	}
	nodes = append(nodes, `"`+hexKey("c")+`"`)
	rec := pollAdminDo(PollingAdminRouter(st, PollingAdminConfig{}), "PUT", "/state", `{"paused":false,"excludedNodes":[`+strings.Join(nodes, ",")+`]}`, "application/json")
	if rec.Code != 200 || len(st.excluded) != 2 || st.excluded[0][0] != 0xbb || st.excluded[1][0] != 0xcc {
		t.Fatal(rec.Code, rec.Body.String(), st.excluded)
	}
}

func TestPollingAdminTargets(t *testing.T) {
	rank, ms := 0, int64(1700000000000)
	st := &pollAdminStub{targets: []api.PollingTarget{{PublicKey: hexKey("a"), LatestReading: &ms,
		Candidates: []api.PollingTargetCandidate{{ObserverPublicKey: hexKey("1"), Rank: &rank, KeyMode: "own"}, {ObserverPublicKey: hexKey("2")}}}}}
	before := time.Now()
	rec := pollAdminDo(PollingAdminRouter(st, PollingAdminConfig{}), "GET", "/targets", "", "")
	after := time.Now()
	if rec.Code != 200 {
		t.Fatal(rec.Code, rec.Body.String())
	}
	if st.tLimit != 500 || st.tAfter.Before(before.Add(-3900*time.Second)) || st.tAfter.After(after.Add(-3900*time.Second)) {
		t.Fatal("bad store args", st.tAfter, st.tLimit)
	}
	for _, want := range []string{`"targets":[{"publicKey":"` + hexKey("a"), `"latestTrustedReading":null`, `"latestReading":1700000000000`,
		`"rank":0`, `"rank":null`, `"keyMode":"own"`, `"lastOk":null`, `"evidenceSnr":null`} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("missing %s in %s", want, rec.Body.String())
		}
	}
	rec = pollAdminDo(PollingAdminRouter(&pollAdminStub{}, PollingAdminConfig{}), "GET", "/targets", "", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"targets":[]`) {
		t.Fatal(rec.Code, rec.Body.String())
	}
	if rec := pollAdminDo(PollingAdminRouter(&pollAdminStub{err: errors.New("x")}, PollingAdminConfig{}), "GET", "/targets", "", ""); rec.Code != 500 {
		t.Fatal(rec.Code)
	}
}

func pollAdminKeys(from, n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = make([]byte, 32)
		out[i][31] = byte(from + i)
	}
	return out
}

func TestPollingAdminGetShowsConfig(t *testing.T) {
	cfg := PollingAdminConfig{Authority: "approved", ExcludedNodes: pollAdminKeys(1, 2)}
	rec := pollAdminDo(PollingAdminRouter(&pollAdminStub{}, cfg), "GET", "/", "", "")
	var got api.PollingAdminState
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || rec.Code != 200 {
		t.Fatal(rec.Code, err)
	}
	if got.Authority != "approved" || len(got.ConfigExcludedNodes) != 2 || got.ConfigExcludedNodes[1] != fmt.Sprintf("%064x", 2) {
		t.Fatalf("%s", rec.Body.String())
	}
	rec = pollAdminDo(PollingAdminRouter(&pollAdminStub{}, PollingAdminConfig{}), "GET", "/", "", "")
	if !strings.Contains(rec.Body.String(), `"configExcludedNodes":[]`) {
		t.Fatal(rec.Body.String())
	}
}

func TestPollingAdminExclusionsFitWithConfig(t *testing.T) {
	const ct = "application/json"
	st := &pollAdminStub{}
	h := PollingAdminRouter(st, PollingAdminConfig{ExcludedNodes: pollAdminKeys(1, 16)})
	rec := pollAdminDo(h, "PUT", "/state", fmt.Sprintf(`{"paused":false,"excludedNodes":["%064x"]}`, 17), ct)
	if rec.Code != 400 || st.writes != 0 || !strings.Contains(rec.Body.String(), "17") || !strings.Contains(rec.Body.String(), "16") {
		t.Fatal(rec.Code, st.writes, rec.Body.String())
	}
	// A key config already excludes adds nothing to the union.
	rec = pollAdminDo(h, "PUT", "/state", fmt.Sprintf(`{"paused":false,"excludedNodes":["%064x"]}`, 16), ct)
	if rec.Code != 200 || st.writes != 1 || len(st.excluded) != 1 {
		t.Fatal(rec.Code, st.writes, rec.Body.String())
	}
}

func TestPollingAdminShowsBlockedTargetsAndStops(t *testing.T) {
	st := &pollAdminStub{blocked: []string{hexKey("a")}, stops: []api.PollingAdminStop{{TargetPublicKey: hexKey("a"), ObserverPublicKey: hexKey("b"), ReportedAt: 5}}}
	rec := pollAdminDo(PollingAdminRouter(st, PollingAdminConfig{}), "GET", "/", "", "")
	body := rec.Body.String()
	if rec.Code != 200 || !strings.Contains(body, `"noPasswordBlocked":["`+hexKey("a")+`"]`) || !strings.Contains(body, `"adminStops":[{"targetPublicKey":"`+hexKey("a")+`","observerPublicKey":"`+hexKey("b")+`","reportedAt":5}]`) {
		t.Fatalf("%d %s", rec.Code, body)
	}
	empty := pollAdminDo(PollingAdminRouter(&pollAdminStub{}, PollingAdminConfig{}), "GET", "/", "", "")
	if !strings.Contains(empty.Body.String(), `"noPasswordBlocked":[]`) || !strings.Contains(empty.Body.String(), `"adminStops":[]`) {
		t.Fatalf("empty lists must be arrays: %s", empty.Body)
	}
}

func TestPollingAdminClearRoutes(t *testing.T) {
	st := &pollAdminStub{found: true}
	h := PollingAdminRouter(st, PollingAdminConfig{})
	rec := pollAdminDo(h, "PUT", "/targets/"+hexKey("a"), `{"noPasswordBlocked":false}`, "application/json")
	if rec.Code != 200 || len(st.stopKeys) != 1 || hex.EncodeToString(st.stopKeys[0]) != hexKey("a") {
		t.Fatalf("clear: %d %s", rec.Code, rec.Body)
	}
	for name, c := range map[string]struct{ path, body, ct string }{
		"set true":  {"/targets/" + hexKey("a"), `{"noPasswordBlocked":true}`, "application/json"},
		"bad key":   {"/targets/zz", `{"noPasswordBlocked":false}`, "application/json"},
		"bad media": {"/targets/" + hexKey("a"), `{"noPasswordBlocked":false}`, "text/plain"},
	} {
		w := st.writes
		if rec := pollAdminDo(h, "PUT", c.path, c.body, c.ct); rec.Code < 400 || st.writes != w {
			t.Errorf("%s: %d writes %d->%d", name, rec.Code, w, st.writes)
		}
	}
	rec = pollAdminDo(h, "DELETE", "/admin-stops/"+hexKey("a")+"/"+hexKey("b"), "", "")
	if rec.Code != 204 || len(st.stopKeys) != 2 || hex.EncodeToString(st.stopKeys[1]) != hexKey("b") {
		t.Fatalf("delete stop: %d %s", rec.Code, rec.Body)
	}
	st.found = false
	if rec := pollAdminDo(h, "DELETE", "/admin-stops/"+hexKey("a")+"/"+hexKey("b"), "", ""); rec.Code != 404 {
		t.Fatalf("unknown stop: %d", rec.Code)
	}
	if rec := pollAdminDo(h, "PUT", "/targets/"+hexKey("a"), `{"noPasswordBlocked":false}`, "application/json"); rec.Code != 404 {
		t.Fatalf("unknown target: %d", rec.Code)
	}
	st.err = errors.New("db")
	if rec := pollAdminDo(h, "DELETE", "/admin-stops/"+hexKey("a")+"/"+hexKey("b"), "", ""); rec.Code != 500 || strings.Contains(rec.Body.String(), "db") {
		t.Fatalf("store error: %d %s", rec.Code, rec.Body)
	}
}
