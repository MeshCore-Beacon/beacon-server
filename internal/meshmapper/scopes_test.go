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

type memoryStore struct {
	rows  map[string]Cache
	fail  bool
	saved []scopestore.Entry
}

func (s *memoryStore) GetScopeCatalogue(_ context.Context, iata, url string) (*Cache, error) {
	row, ok := s.rows[iata+url]
	if !ok {
		return nil, nil
	}
	return &row, nil
}
func (s *memoryStore) SaveScopeCatalogue(_ context.Context, iata, url string, next Cache, entries []scopestore.Entry) error {
	if s.fail {
		return errors.New("offline")
	}
	old := s.rows[iata+url]
	if next.Payload == nil {
		next.Payload = old.Payload
	}
	if next.CheckedAt.IsZero() {
		next.CheckedAt = old.CheckedAt
		next.ETag = old.ETag
	}
	s.rows[iata+url] = next
	s.saved = append(s.saved, entries...)
	return nil
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
	cfg := config.MeshMapperScopesConfig{Enabled: true, Sources: map[string]string{"YOW": server.URL}}
	store := &memoryStore{rows: map[string]Cache{}}
	scopes := scopestore.New()
	manual := scopestore.FromName("manual")
	manual.TransportKey = []byte("manual wins")
	imp, err := New(ctx, cfg, store, scopes, []scopestore.Entry{manual})
	if err != nil {
		t.Fatal(err)
	}
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
	if !imp.retryAfter.Equal(now.Add(2 * time.Hour)) {
		t.Fatal("Retry-After ignored")
	}
	restored := scopestore.New()
	restart, err := New(ctx, cfg, store, restored, []scopestore.Entry{manual})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names(restored), names(scopes)) {
		t.Fatal("outage restart lost cached names")
	}
	before := calls
	if err := restart.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if calls != before {
		t.Fatal("restart ignored shared rate-limit cooldown")
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
	store := &memoryStore{rows: map[string]Cache{}}
	imp, err := New(context.Background(), config.MeshMapperScopesConfig{Enabled: true, Sources: map[string]string{"YOW": server.URL}}, store, scopestore.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
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
	store := &memoryStore{rows: map[string]Cache{}}
	for _, iata := range []string{"YOW", "YVR"} {
		store.rows[iata+iata] = Cache{Payload: []byte(strings.ReplaceAll(string(catalogue("can")), "YOW", iata))}
	}
	scopes := scopestore.New()
	_, err := New(ctx, config.MeshMapperScopesConfig{Enabled: true, Sources: map[string]string{"YOW": "YOW", "YVR": "YVR"}}, store, scopes, nil)
	if err != nil || len(scopes.Entries()) != 1 || !reflect.DeepEqual(scopes.Entries()[0].IATAs, []string{"YOW", "YVR"}) {
		t.Fatal(scopes.Entries(), err)
	}
	_, err = New(ctx, config.MeshMapperScopesConfig{Enabled: true, Sources: map[string]string{"YOW": "changed-source"}}, store, scopes, nil)
	if err != nil || len(scopes.Entries()) != 0 {
		t.Fatal("removed/changed source remained active", err)
	}
	// Disabled mode never calls the store, even if URLs remain configured.
	_, err = New(ctx, config.MeshMapperScopesConfig{Sources: map[string]string{"YOW": "YOW"}}, nil, scopes, nil)
	if err != nil {
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
	store := &memoryStore{rows: map[string]Cache{"YOW" + server.URL: {Payload: []byte(`{"invalid":true}`), ETag: `"old"`}}}
	scopes := scopestore.New()
	imp, err := New(context.Background(), config.MeshMapperScopesConfig{Enabled: true, Sources: map[string]string{"YOW": server.URL}}, store, scopes, []scopestore.Entry{scopestore.FromName("manual")})
	if err != nil || !reflect.DeepEqual(names(scopes), []string{"#manual"}) {
		t.Fatal("invalid optional source blocked manual startup", err)
	}
	if err = imp.Refresh(context.Background()); err != nil || !reflect.DeepEqual(names(scopes), []string{"#manual", "#yow"}) {
		t.Fatal("source did not recover on refresh", err, names(scopes))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	imp.sources[0].cache.NextAttempt = time.Time{}
	if err = imp.Refresh(ctx); err != nil {
		t.Fatalf("normal shutdown is a task failure: %v", err)
	}
}
