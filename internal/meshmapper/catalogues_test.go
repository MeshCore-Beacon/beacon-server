// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshmapper

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
)

func TestPublicCataloguesFreshnessAndIsolation(t *testing.T) {
	status := http.StatusOK
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(status)
		if status == http.StatusOK {
			_, _ = w.Write(catalogue("yow", "YOW", "monitored-zero"))
		}
	}))
	defer server.Close()
	cfg := config.MeshMapperScopesConfig{Enabled: true, Sources: map[string]string{"YOW": server.URL}}
	store := &memoryStore{rows: map[string]Cache{}}
	imp, err := New(context.Background(), cfg, store, scopestore.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := imp.Catalogues(); len(got) != 1 || got[0].CheckedAt != 0 || len(got[0].Scopes) != 0 {
		t.Fatal(got)
	}
	now := time.Now()
	if err := imp.refresh(context.Background(), &imp.sources[0], now); err != nil {
		t.Fatal(err)
	}
	old := imp.Catalogues()
	if old[0].Scopes[0].Name != "#yow" || old[0].Scopes[1].Name != "#YOW" || len(old[0].Scopes) != 3 || old[0].Repeaters != 5 || old[0].Scoped != 3 || old[0].FreshUntil <= old[0].CheckedAt {
		t.Fatal(old)
	}
	status = http.StatusServiceUnavailable
	if err := imp.refresh(context.Background(), &imp.sources[0], now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	current := imp.Catalogues()[0]
	if current.LastError != "HTTP 503" || current.CheckedAt != old[0].CheckedAt || current.GeneratedAt != old[0].GeneratedAt || len(current.Scopes) != 3 || old[0].LastError != "" {
		t.Fatal(current, old)
	}
	// Readers only access published immutable data; the race suite exercises concurrent refreshes.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 100 {
			_, _ = json.Marshal(imp.Catalogues())
		}
	}()
	for range 100 {
		imp.publishCatalogues()
	}
	wg.Wait()
	if calls != 2 {
		t.Fatal("reading public metadata performed upstream requests", calls)
	}
	disabled, err := New(context.Background(), config.MeshMapperScopesConfig{}, store, scopestore.New(), nil)
	if err != nil || len(disabled.Catalogues()) != 0 {
		t.Fatal("disabled importer exposed old membership")
	}
	store.rows["YOW"+server.URL] = Cache{Payload: []byte(`{"scopes":"invalid"}`)}
	restored, err := New(context.Background(), cfg, store, scopestore.New(), nil)
	if err != nil || len(restored.Catalogues()[0].Scopes) != 0 || restored.Catalogues()[0].LastError == "" {
		t.Fatal("invalid saved metadata leaked")
	}
}
