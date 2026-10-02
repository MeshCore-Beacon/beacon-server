// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshmapper

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
)

const square = `{"type":"Polygon","coordinates":[[[-76,45],[-75,45],[-75,46],[-76,46],[-76,45]]]}`

func boundaryBody(code, geometry string) string {
	return `{"type":"FeatureCollection","generated_at":"2026-09-25T15:00:00Z","features":[{"type":"Feature","geometry":` + geometry +
		`,"properties":{"code":"` + code + `","name":"Ottawa, CA","center":[-75.7,45.4],"radius_km":42,"has_boundary":true}}]}`
}

type zoneMemoryStore struct {
	iatas   []string
	heard   []string
	rows    map[string]Boundary
	pruned  []string
	fail    bool
	regions map[string]RegionState
	lists   *zoneListMemory
	details map[string]IATADetails
	writes  int
}

func (s *zoneMemoryStore) ListIATADetails(context.Context) ([]IATADetails, error) {
	var out []IATADetails
	for _, iata := range s.iatas {
		d := s.details[iata]
		d.IATA = iata
		out = append(out, d)
	}
	return out, nil
}

func (s *zoneMemoryStore) UpsertIATADetails(_ context.Context, iata, name string, lat, lng *float64) error {
	if s.details == nil {
		s.details = map[string]IATADetails{}
	}
	s.details[iata] = IATADetails{IATA: iata, Name: name, Lat: lat, Lng: lng}
	s.writes++
	return nil
}

func (s *zoneMemoryStore) ListRegionState(context.Context) ([]RegionState, error) {
	var out []RegionState
	for _, r := range s.regions {
		out = append(out, r)
	}
	return out, nil
}

func (s *zoneMemoryStore) SaveImportedRegion(_ context.Context, r RegionState) (bool, error) {
	if s.regions == nil {
		s.regions = map[string]RegionState{}
	}
	if cur, ok := s.regions[r.Slug]; ok && !cur.Imported {
		return false, nil
	}
	r.Imported = true
	s.regions[r.Slug] = r
	for _, m := range r.IATAs { // members become known IATAs, as AddIATAs does
		if !slices.Contains(s.iatas, m) {
			s.iatas = append(s.iatas, m)
		}
	}
	return true, nil
}

func (s *zoneMemoryStore) PruneImportedRegions(_ context.Context, keep []string) ([]string, error) {
	var removed []string
	for slug, r := range s.regions {
		if r.Imported && !slices.Contains(keep, slug) {
			delete(s.regions, slug)
			removed = append(removed, slug)
		}
	}
	return removed, nil
}

func (s *zoneMemoryStore) ListKnownIATAs(context.Context) ([]string, error) {
	return slices.Clone(s.iatas), nil
}

func (s *zoneMemoryStore) ListHeardIATAs(context.Context) ([]string, error) {
	return slices.Clone(s.heard), nil
}

func (s *zoneMemoryStore) PruneZoneBoundaries(_ context.Context, keep []string) ([]string, error) {
	var removed []string
	for iata := range s.rows {
		if !slices.Contains(keep, iata) {
			delete(s.rows, iata)
			removed = append(removed, iata)
		}
	}
	s.pruned = removed
	return removed, nil
}

func (s *zoneMemoryStore) ListZoneBoundaries(context.Context) ([]Boundary, error) {
	var out []Boundary
	for _, b := range s.rows {
		out = append(out, b)
	}
	return out, nil
}

func (s *zoneMemoryStore) SaveZoneBoundary(_ context.Context, b Boundary) error {
	if s.fail {
		return errors.New("offline")
	}
	old := s.rows[b.IATA]
	if b.Feature == nil {
		b.Feature = old.Feature
	}
	if b.CheckedAt.IsZero() {
		b.CheckedAt, b.ETag = old.CheckedAt, old.ETag
	}
	s.rows[b.IATA] = b
	return nil
}

type zoneListMemory struct{ rows map[string]ZoneList }

func newZoneListMemory() *zoneListMemory { return &zoneListMemory{rows: map[string]ZoneList{}} }

func (s *zoneListMemory) ListZoneLists(context.Context) ([]ZoneList, error) {
	var out []ZoneList
	for _, l := range s.rows {
		out = append(out, l)
	}
	return out, nil
}

func (s *zoneListMemory) SaveZoneList(_ context.Context, l ZoneList) error {
	old := s.rows[l.Country]
	if l.Payload == nil {
		l.Payload = old.Payload
	}
	if l.FetchedAt.IsZero() {
		l.FetchedAt, l.ETag = old.FetchedAt, old.ETag
	}
	s.rows[l.Country] = l
	return nil
}

type fakeMeshMapper struct {
	*httptest.Server
	list, boundary     string
	listStatus, status int
	etag, retryAfter   string
	listCalls, calls   int
	ifNoneMatch        []string
	otherStatus        int         // get_zones.php status for countries other than CA; 0 is unexpected
	hang               atomic.Bool // requests wait until the client gives up
	hung               atomic.Int32
}

func newFakeMeshMapper(t *testing.T) *fakeMeshMapper {
	f := &fakeMeshMapper{listStatus: 200, status: 200, etag: `"v1"`, boundary: boundaryBody("YOW", square)}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.hang.Load() {
			f.hung.Add(1)
			<-r.Context().Done()
			return
		}
		switch r.URL.Path {
		case "/get_zones.php":
			f.listCalls++
			if r.URL.Query().Get("country") != "CA" {
				if f.otherStatus == 0 {
					t.Errorf("country query: %s", r.URL.RawQuery)
				}
				w.WriteHeader(f.otherStatus)
				return
			}
			w.WriteHeader(f.listStatus)
			_, _ = w.Write([]byte(f.list))
		case "/get_geojson.php":
			f.calls++
			f.ifNoneMatch = append(f.ifNoneMatch, r.Header.Get("If-None-Match"))
			w.Header().Set("ETag", f.etag)
			w.Header().Set("Retry-After", f.retryAfter)
			w.WriteHeader(f.status)
			if f.status == 200 {
				_, _ = w.Write([]byte(f.boundary))
			}
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	f.list = `{"country":"CA","count":1,"zones":[{"code":"YOW","url":"` + f.URL + `/","has_boundary":true,"group":null}],"groups":[]}`
	t.Cleanup(f.Close)
	return f
}

type zoneHarness struct {
	z        *Zones
	store    *zoneMemoryStore
	changed  []string
	imported map[string]json.RawMessage
}

func newZoneHarness(t *testing.T, f *fakeMeshMapper, store *zoneMemoryStore, enabled bool) *zoneHarness {
	t.Helper()
	h := &zoneHarness{store: store}
	if store.iatas == nil {
		store.iatas = []string{"YOW"}
	}
	if store.lists == nil {
		store.lists = newZoneListMemory()
	}
	dir := NewDirectory(store.lists)
	dir.listURL = f.URL + "/get_zones.php"
	if err := dir.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.z = NewZones(config.MeshMapperZonesConfig{Enabled: enabled}, store, dir)
	h.z.boundsURL = func(site string) (string, bool) { return site + "get_geojson.php", strings.HasPrefix(site, f.URL) }
	h.z.OnChange(func(_ context.Context, iata string) { h.changed = append(h.changed, iata) })
	h.z.OnUpdate(func(imported map[string]json.RawMessage) { h.imported = imported })
	if err := h.z.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	return h
}

// tick forces the region due and runs refreshes until the boundary request is made.
func (h *zoneHarness) tick(t *testing.T) Boundary {
	t.Helper()
	h.z.regions[0].b.NextAttempt = time.Time{}
	for range 2 {
		if err := h.z.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return h.store.rows["YOW"]
}

func TestZonesImportAndConditionalRefresh(t *testing.T) {
	f := newFakeMeshMapper(t)
	h := newZoneHarness(t, f, &zoneMemoryStore{rows: map[string]Boundary{}}, true)
	b := h.tick(t)
	if f.listCalls != 1 || f.calls != 1 || b.Feature == nil || b.ETag != `"v1"` || b.LastError != "" || b.URL != f.URL+"/" {
		t.Fatalf("import failed: %+v list=%d calls=%d", b, f.listCalls, f.calls)
	}
	if !slices.Equal(h.changed, []string{"YOW"}) || h.imported["YOW"] == nil {
		t.Fatal("listeners not told", h.changed, h.imported)
	}
	if !strings.Contains(string(b.Feature), `"bbox"`) || !strings.Contains(string(b.Feature), `"code":"YOW"`) {
		t.Fatal("feature not validated with bbox and properties", string(b.Feature))
	}
	if got := b.NextAttempt.Sub(b.AttemptedAt); got != 24*time.Hour {
		t.Fatal("refresh interval", got)
	}

	f.status = 304
	b = h.tick(t)
	if f.listCalls != 1 || f.ifNoneMatch[1] != `"v1"` || b.Feature == nil || b.LastError != "" || len(h.changed) != 1 {
		t.Fatalf("304 mishandled: %+v list=%d changed=%v", b, f.listCalls, h.changed)
	}

	f.status, f.etag = 200, `"v2"`
	f.boundary = boundaryBody("YOW", `{"type":"Polygon","coordinates":[[[-77,45],[-75,45],[-75,46],[-77,46],[-77,45]]]}`)
	b = h.tick(t)
	if b.ETag != `"v2"` || len(h.changed) != 2 || !strings.Contains(string(h.imported["YOW"]), "-77") {
		t.Fatal("changed boundary not published", b.ETag, h.changed)
	}
}

func TestZonesKeepLastGoodBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, boundary string
		status         int
		wantErr        string
		wantRetry      time.Duration
	}{
		{"null geometry", boundaryBody("YOW", "null"), 200, "no boundary", 24 * time.Hour},
		{"not found", "", 404, "HTTP 404", 24 * time.Hour},
		{"unavailable", "", 503, "HTTP 503", 24 * time.Hour},
		{"truncated", boundaryBody("YOW", square)[:80], 200, "invalid response", 24 * time.Hour},
		{"other region", boundaryBody("YVR", square), 200, "invalid response", 24 * time.Hour},
		{"oversized", `{"type":"FeatureCollection","features":[],"pad":"` + strings.Repeat("x", MaxBoundaryBody) + `"}`, 200, "invalid response", 24 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeMeshMapper(t)
			h := newZoneHarness(t, f, &zoneMemoryStore{rows: map[string]Boundary{}}, true)
			good := h.tick(t).Feature
			f.status, f.boundary = tc.status, tc.boundary
			b := h.tick(t)
			if b.LastError != tc.wantErr || string(b.Feature) != string(good) || string(h.imported["YOW"]) != string(good) || len(h.changed) != 1 {
				t.Fatalf("got %q, feature kept=%v", b.LastError, string(b.Feature) == string(good))
			}
			if got := b.NextAttempt.Sub(b.AttemptedAt); got != tc.wantRetry {
				t.Fatal("retry", got)
			}
		})
	}
}

func TestZonesSkipRegionsWithoutBoundary(t *testing.T) {
	for _, tc := range []struct{ name, zones, want string }{
		{"not listed", `[]`, "not listed"},
		{"no boundary", `[{"code":"YOW","url":"URL/","has_boundary":false}]`, "no boundary"},
		{"foreign site", `[{"code":"yow","url":"https://example.com/","has_boundary":true}]`, "invalid site URL"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeMeshMapper(t)
			f.list = `{"country":"CA","count":1,"zones":` + strings.ReplaceAll(tc.zones, "URL", f.URL) + `,"groups":[]}`
			h := newZoneHarness(t, f, &zoneMemoryStore{rows: map[string]Boundary{}}, true)
			b := h.tick(t)
			if f.calls != 0 || b.LastError != tc.want || b.Feature != nil || len(h.changed) != 0 {
				t.Fatalf("got %+v calls=%d", b, f.calls)
			}
			if got := b.NextAttempt.Sub(b.AttemptedAt); got != 24*time.Hour {
				t.Fatal("authoritative answer retried early", got)
			}
		})
	}
}

func TestZonesListFailureBacksOff(t *testing.T) {
	f := newFakeMeshMapper(t)
	f.listStatus = 500
	h := newZoneHarness(t, f, &zoneMemoryStore{rows: map[string]Boundary{}}, true)
	for range 3 {
		if err := h.z.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if f.listCalls != 1 || f.calls != 0 || len(h.store.rows) != 0 {
		t.Fatal("failed list retried early", f.listCalls, f.calls)
	}
	f.listStatus, f.list = 200, `{"country":"US","zones":[]}`
	h.z.dir.lists["CA"].nextAttempt, h.z.dir.lists["CA"].fetchedAt = time.Time{}, time.Time{}
	_ = h.z.Refresh(context.Background())
	if h.z.dir.lists["CA"].zones != nil {
		t.Fatal("list for another country accepted")
	}
}

func TestZonesRateLimitWaitsOnlyForThatRegion(t *testing.T) {
	for retryAfter, want := range map[string]time.Duration{"120": 24 * time.Hour, "172800": 48 * time.Hour} {
		f := newFakeMeshMapper(t)
		h := newZoneHarness(t, f, &zoneMemoryStore{rows: map[string]Boundary{}}, true)
		f.status, f.retryAfter = 429, retryAfter
		b := h.tick(t)
		if b.LastError != "HTTP 429" || b.NextAttempt.Sub(b.AttemptedAt) != want {
			t.Fatalf("Retry-After %ss: next attempt in %v, want %v", retryAfter, b.NextAttempt.Sub(b.AttemptedAt), want)
		}
		restarted := newZoneHarness(t, f, h.store, true)
		if err := restarted.z.Refresh(context.Background()); err != nil || f.calls != 1 {
			t.Fatal("requested again before the rate limit allows", err, f.calls)
		}
	}
}

func TestZoneListSurvivesRestart(t *testing.T) {
	f := newFakeMeshMapper(t)
	store := &zoneMemoryStore{rows: map[string]Boundary{}}
	h := newZoneHarness(t, f, store, true)
	h.tick(t)
	if f.listCalls != 1 {
		t.Fatal("list not fetched", f.listCalls)
	}
	if got := h.z.dir.lists["CA"].nextAttempt.Sub(h.z.dir.lists["CA"].fetchedAt); got != 24*time.Hour {
		t.Fatal("zone list refetched sooner than the rate limit allows", got)
	}
	restarted := newZoneHarness(t, f, store, true)
	restarted.tick(t)
	if f.listCalls != 1 || f.calls != 2 {
		t.Fatal("restart refetched the zone list", f.listCalls, f.calls)
	}
}

func TestZonesRestoreAndPrune(t *testing.T) {
	f := newFakeMeshMapper(t)
	store := &zoneMemoryStore{rows: map[string]Boundary{}}
	first := newZoneHarness(t, f, store, true)
	first.tick(t)
	store.rows["OLD"] = Boundary{IATA: "OLD", Feature: json.RawMessage(`{}`)}

	restarted := newZoneHarness(t, f, store, true)
	if !slices.Equal(store.pruned, []string{"OLD"}) || !slices.Equal(restarted.changed, []string{"OLD"}) {
		t.Fatal("unknown IATA not pruned", store.pruned, restarted.changed)
	}
	if restarted.imported["YOW"] == nil || f.calls != 1 {
		t.Fatal("saved boundary not restored without a request")
	}
	if err := restarted.z.Refresh(context.Background()); err != nil || f.calls != 1 || f.listCalls != 1 {
		t.Fatal("refreshed before due", err, f.calls)
	}

	disabled := newZoneHarness(t, f, store, false)
	if len(store.rows) != 0 || !slices.Equal(disabled.changed, []string{"YOW"}) || len(disabled.z.regions) != 0 {
		t.Fatal("disabling did not remove imports")
	}
}

func TestZonesPickUpNewIATAs(t *testing.T) {
	f := newFakeMeshMapper(t)
	store := &zoneMemoryStore{iatas: []string{}, rows: map[string]Boundary{}}
	h := newZoneHarness(t, f, store, true)
	for range 2 {
		if err := h.z.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if f.listCalls != 0 || f.calls != 0 {
		t.Fatal("requested with no IATAs", f.listCalls, f.calls)
	}
	store.iatas = []string{"YOW", "ZZZ"}
	for range 2 {
		if err := h.z.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(h.z.regions) != 1 || h.imported["YOW"] == nil || !slices.Equal(h.changed, []string{"YOW"}) {
		t.Fatal("new IATA not imported", len(h.z.regions), h.changed)
	}
}

func TestZonesPersistFailureWaitsForNextAttempt(t *testing.T) {
	f := newFakeMeshMapper(t)
	h := newZoneHarness(t, f, &zoneMemoryStore{rows: map[string]Boundary{}, fail: true}, true)
	h.z.regions[0].b.NextAttempt = time.Time{}
	_ = h.z.Refresh(context.Background())
	if err := h.z.Refresh(context.Background()); err == nil {
		t.Fatal("persist error hidden")
	}
	if h.z.regions[0].b.NextAttempt.IsZero() || h.imported["YOW"] != nil || len(h.changed) != 0 {
		t.Fatal("uncommitted boundary published")
	}
}

func TestBoundaryEndpoint(t *testing.T) {
	for site, want := range map[string]string{
		"https://yow.meshmapper.net/":          "https://yow.meshmapper.net/get_geojson.php",
		"https://yow.meshmapper.net":           "https://yow.meshmapper.net/get_geojson.php",
		"http://yow.meshmapper.net/":           "",
		"https://meshmapper.net.evil.com/":     "",
		"https://user@yow.meshmapper.net/":     "",
		"https://yow.meshmapper.net/x/":        "",
		"https://yow.meshmapper.net/?a=b":      "",
		"https://yow.meshmapper.net:8443/":     "",
		"https://yow.meshmapper.net/#fragment": "",
	} {
		got, ok := boundaryEndpoint(site)
		if got != want || ok != (want != "") {
			t.Errorf("%s: %q %v", site, got, ok)
		}
	}
}

func TestDecodeBoundaryRejectsUnusableOutlines(t *testing.T) {
	for name, body := range map[string]string{
		"two features":  `{"type":"FeatureCollection","features":[{"type":"Feature","geometry":null,"properties":{"code":"YOW"}},{"type":"Feature","geometry":null,"properties":{"code":"YOW"}}]}`,
		"not a polygon": boundaryBody("YOW", `{"type":"Point","coordinates":[-75,45]}`),
		"open ring":     boundaryBody("YOW", `{"type":"Polygon","coordinates":[[[-76,45],[-75,45],[-75,46],[-76,46]]]}`),
		"antimeridian":  boundaryBody("YOW", `{"type":"Polygon","coordinates":[[[179,10],[-179,10],[-179,20],[179,20],[179,10]]]}`),
		"zero area":     boundaryBody("YOW", `{"type":"Polygon","coordinates":[[[-76,45],[-75,45],[-74,45],[-76,45]]]}`),
	} {
		if _, err := decodeBoundary([]byte(body), "YOW"); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if feature, err := decodeBoundary([]byte(boundaryBody("yow", square)), "YOW"); err != nil || feature == nil {
		t.Fatal("case-insensitive code rejected", err)
	}
}

func TestClientsAllowMeshMapperTimeout(t *testing.T) {
	ctx := context.Background()
	scopes, err := New(ctx, config.MeshMapperScopesConfig{}, nil, NewDirectory(nil), scopestore.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	channels, err := NewChannels(ctx, config.MeshMapperChannelsConfig{}, newChannelMemoryStore(), NewDirectory(nil), keystore.NewMapKeyStore(nil))
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]*http.Client{
		"directory": NewDirectory(nil).client,
		"zones":     NewZones(config.MeshMapperZonesConfig{}, nil, nil).client,
		"scopes":    scopes.client,
		"channels":  channels.client,
	} {
		if c.Timeout < 60*time.Second {
			t.Errorf("%s client gives up after %v; MeshMapper asks for 60-120s", name, c.Timeout)
		}
	}
	if refreshTimeout <= requestTimeout {
		t.Fatal("a refresh must outlast its request")
	}
}

func TestZoneListCutShortStillCounts(t *testing.T) {
	f := newFakeMeshMapper(t)
	f.hang.Store(true)
	lists := newZoneListMemory()
	dir := NewDirectory(lists)
	dir.listURL = f.URL + "/get_zones.php"
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, fetched, _ := dir.List(ctx, "CA", now); !fetched {
		t.Fatal("first call should spend the request")
	}
	f.hang.Store(false)
	if zones, fetched, err := dir.List(context.Background(), "CA", now.Add(15*time.Second)); zones != nil || fetched || err != nil || f.listCalls != 0 || f.hung.Load() != 1 {
		t.Fatal("abandoned request retried inside the rate limit", f.listCalls, fetched, err)
	}
	if got := lists.rows["CA"].NextAttempt.Sub(now); got != zoneListFresh {
		t.Fatal("abandoned request not persisted", lists.rows["CA"])
	}
}

func TestZoneListFetchDoesNotBlockOtherCallers(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-release
		_, _ = w.Write([]byte(`{"country":"CA","zones":[]}`))
	}))
	defer server.Close()
	defer close(release)
	dir := NewDirectory(newZoneListMemory())
	dir.listURL = server.URL
	now := time.Now().UTC()
	go func() { _, _, _ = dir.List(context.Background(), "CA", now) }()
	for calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if zones, fetched, err := dir.List(context.Background(), "CA", now); zones != nil || fetched || err != nil {
			t.Error("second caller should back off while the list is in flight", fetched, err)
		}
		_, _, _, _ = dir.snapshot([]string{"CA"})
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("caller waited on another task's request")
	}
	if calls.Load() != 1 {
		t.Fatal("duplicate list request", calls.Load())
	}
}

func TestBoundaryCutShortStillCounts(t *testing.T) {
	f := newFakeMeshMapper(t)
	h := newZoneHarness(t, f, &zoneMemoryStore{rows: map[string]Boundary{}}, true)
	if err := h.z.Refresh(context.Background()); err != nil || f.listCalls != 1 {
		t.Fatal("list not fetched", err)
	}
	f.hang.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = h.z.Refresh(ctx)
	f.hang.Store(false)
	if err := h.z.Refresh(context.Background()); err != nil || f.calls != 0 || f.hung.Load() != 1 {
		t.Fatal("abandoned boundary request retried inside the rate limit", err, f.calls)
	}
	if b := h.store.rows["YOW"]; b.NextAttempt.Sub(b.AttemptedAt) != 24*time.Hour {
		t.Fatal("abandoned boundary request not persisted", b)
	}
}
