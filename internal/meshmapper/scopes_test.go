// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshmapper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
)

func catalogue(names ...string) []byte {
	rows := make([]map[string]any, 0, len(names))
	for _, name := range names {
		rows = append(rows, map[string]any{"name": name, "repeaters": 0, "default": 0, "monitored": true, "wardriving": false})
	}
	b, _ := json.Marshal(map[string]any{"generated_at": "2026-09-27T16:58:49Z", "region": "YOW", "zones": []string{"YOW"}, "repeaters": 5, "scoped": 3, "scopes": rows})
	return b
}

type sourceKey struct{ iata, url string }

type memoryStore struct {
	iatas []string
	rows  map[sourceKey]Cache
	fail  bool
	saved []scopestore.Entry
}

func newMemoryStore(iatas ...string) *memoryStore {
	return &memoryStore{iatas: iatas, rows: map[sourceKey]Cache{}}
}

func (s *memoryStore) ListKnownIATAs(context.Context) ([]string, error) {
	return slices.Clone(s.iatas), nil
}

func (s *memoryStore) ListScopeCatalogues(context.Context) ([]Catalogue, error) {
	var out []Catalogue
	for key, c := range s.rows {
		out = append(out, Catalogue{IATA: key.iata, URL: key.url, Cache: c})
	}
	return out, nil
}

func (s *memoryStore) SaveScopeCatalogue(_ context.Context, iata, url string, next Cache, entries []scopestore.Entry) error {
	if s.fail {
		return errors.New("offline")
	}
	old := s.rows[sourceKey{iata, url}]
	if next.Payload == nil {
		next.Payload = old.Payload
	}
	if next.CheckedAt.IsZero() {
		next.CheckedAt = old.CheckedAt
		next.ETag = old.ETag
	}
	s.rows[sourceKey{iata, url}] = next
	s.saved = append(s.saved, entries...)
	return nil
}

// newImporter restores an importer for YOW whose source points at url.
func newImporter(t *testing.T, cfg config.MeshMapperScopesConfig, store *memoryStore, url string, scopes *scopestore.ScopeStore, manual ...scopestore.Entry) *Importer {
	t.Helper()
	imp, err := New(context.Background(), cfg, store, NewDirectory(newZoneListMemory()), scopes, manual)
	if err != nil {
		t.Fatal(err)
	}
	if len(imp.sources) != 1 {
		t.Fatal("expected one source", imp.sources)
	}
	imp.sources[0].url = url
	return imp
}

func names(store *scopestore.ScopeStore) []string {
	out := []string{}
	for _, e := range store.Entries() {
		out = append(out, e.Name)
	}
	return out
}

func TestCatalogueRefreshFallbackAndRestart(t *testing.T) {
	ctx := context.Background()
	status, body, etag, after := 200, catalogue("yow", "manual"), `"v1"`, ""
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls > 1 && r.Header.Get("If-None-Match") != `"v1"` {
			t.Errorf("missing conditional refresh: %v", r.Header)
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Retry-After", after)
		w.WriteHeader(status)
		if status != 304 {
			_, _ = w.Write(body)
		}
	}))
	defer server.Close()
	cfg := config.MeshMapperScopesConfig{Enabled: true}
	store := newMemoryStore("YOW")
	scopes := scopestore.New()
	manual := scopestore.FromName("manual")
	manual.TransportKey = []byte("manual wins")
	imp := newImporter(t, cfg, store, server.URL, scopes, manual)
	now := time.Now().UTC()
	check := func() {
		t.Helper()
		now = now.Add(time.Minute)
		if err := imp.refresh(ctx, &imp.sources[0], now); err != nil {
			t.Fatal(err)
		}
	}
	check()
	if got := names(scopes); !reflect.DeepEqual(got, []string{"#manual", "#yow"}) {
		t.Fatal(got)
	}
	if string(scopes.Entries()[0].TransportKey) != "manual wins" || scopes.Entries()[0].IATAs != nil {
		t.Fatal("manual key replaced/restricted")
	}
	if !reflect.DeepEqual(scopes.Entries()[1].IATAs, []string{"YOW"}) {
		t.Fatal("import not bound to region")
	}
	if got := scopes.NamesForIATAs([]string{"YOW"}); !reflect.DeepEqual(got, []string{"#manual", "#yow"}) {
		t.Fatal("catalogue membership must include manually overridden names", got)
	}
	generated := imp.sources[0].generated
	status = 304
	check()
	checked := imp.sources[0].cache.CheckedAt
	if checked != now || imp.sources[0].generated != generated {
		t.Fatal("304 lost freshness or changed generation time")
	}
	for _, failure := range []int{200, 404, 503, 302} {
		status, body = failure, []byte(`{"scopes":null}`)
		check()
		if imp.sources[0].cache.CheckedAt != checked || len(scopes.Entries()) != 2 {
			t.Fatal("failed refresh replaced known catalogue")
		}
	}
	status, after = 429, "7200"
	check()
	if !imp.sources[0].cache.NextAttempt.Equal(now.Add(2 * time.Hour)) {
		t.Fatal("Retry-After ignored")
	}
	restored := scopestore.New()
	restart := newImporter(t, cfg, store, server.URL, restored, manual)
	if !reflect.DeepEqual(names(restored), names(scopes)) {
		t.Fatal("outage restart lost cached names")
	}
	before := calls
	if err := restart.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != before {
		t.Fatal("restart retried before Retry-After")
	}
	status, after, body = 200, "", catalogue()
	check()
	if got := names(scopes); !reflect.DeepEqual(got, []string{"#manual"}) {
		t.Fatalf("empty snapshot: %v", got)
	}
	store.fail = true
	body = catalogue("new")
	now = now.Add(time.Hour)
	if err := imp.refresh(ctx, &imp.sources[0], now); err == nil || len(scopes.Entries()) != 1 {
		t.Fatal("uncommitted names became active")
	}
}

func TestDecodeCatalogueBoundaries(t *testing.T) {
	valid := string(catalogue("yow", "YOW", "$private"))
	bad := []string{
		`null`, `{}`, `[]`, valid + `{}`,
		strings.Replace(valid, `"region":"YOW"`, `"region":"PNW"`, 1),
		strings.Replace(valid, `"zones":["YOW"]`, `"zones":["YOW","YVR"]`, 1),
		strings.Replace(valid, `"scoped":3`, `"scoped":6`, 1),
		strings.Replace(valid, `"repeaters":0`, `"repeaters":-1`, 1),
		strings.Replace(valid, `"monitored":true,`, ``, 1),
		strings.Replace(valid, `"generated_at":"2026-09-27T16:58:49Z"`, `"generated_at":null`, 1),
		string(catalogue("yow", "#yow")), string(catalogue("#")), string(catalogue(" yow")), string(catalogue("a\nb")),
		strings.Repeat(" ", MaxBody+1),
	}
	tooMany := []string{}
	for n := 0; n <= MaxScopes; n++ {
		tooMany = append(tooMany, fmt.Sprint(n))
	}
	bad = append(bad, string(catalogue(tooMany...)))
	for n, raw := range bad {
		if _, _, err := decode([]byte(raw), "YOW"); err == nil {
			t.Errorf("invalid case %d accepted", n)
		}
	}
	entries, _, err := decode([]byte(valid), "YOW")
	if err != nil || len(entries) != 3 {
		t.Fatal(entries, err)
	}
	if entries[0].Name != "#yow" || entries[1].Name != "#YOW" || entries[2].Name != "$private" {
		t.Fatal("case/prefix changed", entries)
	}
}

func TestNoRedirectAndResponseBounds(t *testing.T) {
	targetCalls := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls++ }))
	defer target.Close()
	var mode atomic.Value
	mode.Store("redirect")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch mode.Load().(string) {
		case "redirect":
			http.Redirect(w, r, target.URL, 302)
		case "oversize":
			_, _ = w.Write([]byte(strings.Repeat("x", MaxBody+1)))
		case "timeout":
			select {
			case <-r.Context().Done():
			case <-time.After(time.Second):
			}
		case "304":
			w.WriteHeader(304)
		}
	}))
	defer server.Close()
	imp := newImporter(t, config.MeshMapperScopesConfig{Enabled: true}, newMemoryStore("YOW"), server.URL, scopestore.New())
	imp.client.Timeout = 20 * time.Millisecond
	for _, value := range []string{"redirect", "oversize", "timeout", "304"} {
		mode.Store(value)
		if err := imp.refresh(context.Background(), &imp.sources[0], time.Now()); err != nil {
			t.Fatal(err)
		}
		if imp.sources[0].cache.LastError == "" || len(imp.sources[0].entries) != 0 {
			t.Fatal("invalid response accepted", value)
		}
	}
	if targetCalls != 0 {
		t.Fatal("followed redirect")
	}
}

func TestImportedSourcesOverlapWithoutGlobalMembership(t *testing.T) {
	ctx := context.Background()
	store := newMemoryStore("YOW", "YVR")
	for _, iata := range []string{"YOW", "YVR"} {
		store.rows[sourceKey{iata, "old-" + iata}] = Cache{Payload: []byte(`{}`)}
		store.rows[sourceKey{iata, iata}] = Cache{Payload: []byte(strings.ReplaceAll(string(catalogue("can")), "YOW", iata)), AttemptedAt: time.Now()}
	}
	scopes := scopestore.New()
	_, err := New(ctx, config.MeshMapperScopesConfig{Enabled: true}, store, NewDirectory(newZoneListMemory()), scopes, nil)
	if err != nil || len(scopes.Entries()) != 1 || !reflect.DeepEqual(scopes.Entries()[0].IATAs, []string{"YOW", "YVR"}) {
		t.Fatal("latest saved catalogue per IATA not restored", scopes.Entries(), err)
	}
	if got := scopes.NamesForIATAs([]string{"YVR"}); !reflect.DeepEqual(got, []string{"#can"}) {
		t.Fatal(got)
	}
	// Disabled mode never calls the store.
	disabled := scopestore.New()
	if _, err = New(ctx, config.MeshMapperScopesConfig{}, nil, NewDirectory(newZoneListMemory()), disabled, nil); err != nil || len(disabled.Entries()) != 0 {
		t.Fatal(err)
	}
}

func TestInvalidSavedCatalogueDoesNotBlockStartupOrRefresh(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != "" {
			t.Error("invalid saved catalogue must not accept a 304")
		}
		_, _ = w.Write(catalogue("yow"))
	}))
	defer server.Close()
	store := newMemoryStore("YOW")
	store.rows[sourceKey{"YOW", server.URL}] = Cache{Payload: []byte(`{"invalid":true}`), ETag: `"old"`}
	scopes := scopestore.New()
	imp := newImporter(t, config.MeshMapperScopesConfig{Enabled: true}, store, server.URL, scopes, scopestore.FromName("manual"))
	if !reflect.DeepEqual(names(scopes), []string{"#manual"}) {
		t.Fatal("invalid optional source blocked manual startup")
	}
	if err := imp.refresh(context.Background(), &imp.sources[0], time.Now()); err != nil || !reflect.DeepEqual(names(scopes), []string{"#manual", "#yow"}) {
		t.Fatal("source did not recover on refresh", err, names(scopes))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	imp.sources[0].cache.NextAttempt = time.Time{}
	if err := imp.Refresh(ctx); err != nil {
		t.Fatalf("normal shutdown is a task failure: %v", err)
	}
}

func TestRetryAfterNeverBeatsTheRateLimit(t *testing.T) {
	for retryAfter, want := range map[string]time.Duration{"1800": time.Hour, "7200": 2 * time.Hour} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", retryAfter)
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		imp := newImporter(t, config.MeshMapperScopesConfig{Enabled: true}, newMemoryStore("YOW"), server.URL, scopestore.New())
		now := time.Now().UTC()
		if err := imp.refresh(context.Background(), &imp.sources[0], now); err != nil {
			t.Fatal(err)
		}
		server.Close()
		if got := imp.sources[0].cache.NextAttempt.Sub(now); got != want {
			t.Fatalf("Retry-After %ss: next attempt in %v, want %v", retryAfter, got, want)
		}
	}
}

func TestFailedRefreshRetriesSoon(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }))
	defer server.Close()
	imp := newImporter(t, config.MeshMapperScopesConfig{Enabled: true}, newMemoryStore("YOW"), server.URL, scopestore.New())
	now := time.Now().UTC()
	if err := imp.refresh(ctx, &imp.sources[0], now); err != nil {
		t.Fatal(err)
	}
	if got := imp.sources[0].cache.NextAttempt.Sub(now); got != time.Hour {
		t.Fatalf("next attempt after failure in %v, want 1h", got)
	}
}

// fakeSites serves a CA zone list and YOW's scope catalogue.
type fakeSites struct {
	*httptest.Server
	list                 string
	listCalls, scopeHits int
}

func newFakeSites(t *testing.T) *fakeSites {
	f := &fakeSites{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/get_zones.php":
			f.listCalls++
			_, _ = w.Write([]byte(f.list))
		case "/get_scopes.php":
			f.scopeHits++
			_, _ = w.Write(catalogue("yow"))
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	f.list = `{"country":"CA","zones":[{"code":"YOW","url":"` + f.URL + `/","has_boundary":true,"group":null}]}`
	t.Cleanup(f.Close)
	return f
}

func newDiscoveringImporter(t *testing.T, f *fakeSites, store *memoryStore, scopes *scopestore.ScopeStore) *Importer {
	t.Helper()
	dir := NewDirectory(newZoneListMemory())
	dir.listURL = f.URL + "/get_zones.php"
	imp, err := New(context.Background(), config.MeshMapperScopesConfig{Enabled: true}, store, dir, scopes, nil)
	if err != nil {
		t.Fatal(err)
	}
	imp.scopesURL = func(site string) (string, bool) { return site + "get_scopes.php", strings.HasPrefix(site, f.URL) }
	return imp
}

func refreshN(t *testing.T, imp *Importer, n int) {
	t.Helper()
	for range n {
		if err := imp.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestScopesDiscoverSourcesFromZoneList(t *testing.T) {
	f := newFakeSites(t)
	store := newMemoryStore()
	scopes := scopestore.New()
	imp := newDiscoveringImporter(t, f, store, scopes)
	refreshN(t, imp, 2)
	if f.listCalls != 0 || f.scopeHits != 0 {
		t.Fatal("requested with no IATAs", f.listCalls, f.scopeHits)
	}
	store.iatas = []string{"YOW", "ZZZ"}
	refreshN(t, imp, 3)
	if f.listCalls != 1 || f.scopeHits != 1 || !reflect.DeepEqual(names(scopes), []string{"#yow"}) {
		t.Fatal("source not discovered", f.listCalls, f.scopeHits, names(scopes))
	}
	if _, ok := store.rows[sourceKey{"YOW", f.URL + "/get_scopes.php"}]; !ok {
		t.Fatal("catalogue not saved under the discovered URL", store.rows)
	}
}

func TestScopesSkipUnlistedIATAs(t *testing.T) {
	f := newFakeSites(t)
	f.list = `{"country":"CA","zones":[]}`
	imp := newDiscoveringImporter(t, f, newMemoryStore("YOW"), scopestore.New())
	refreshN(t, imp, 3)
	s := imp.sources[0]
	if f.scopeHits != 0 || s.cache.LastError != "not listed" || s.cache.NextAttempt.Sub(time.Now()) < 59*time.Minute {
		t.Fatalf("unlisted IATA requested or retried early: hits=%d %+v", f.scopeHits, s.cache)
	}
}

func TestScopesRequestCutShortStillCounts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	store := newMemoryStore("YOW")
	imp := newImporter(t, config.MeshMapperScopesConfig{Enabled: true}, store, server.URL, scopestore.New())
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = imp.refresh(ctx, &imp.sources[0], now)
	if got := imp.sources[0].cache.NextAttempt.Sub(now); got != failureRetry {
		t.Fatal("abandoned request retried inside the rate limit", got)
	}
	if row := store.rows[sourceKey{"YOW", server.URL}]; row.NextAttempt.Sub(now) != failureRetry {
		t.Fatal("abandoned request not persisted", row)
	}
}
