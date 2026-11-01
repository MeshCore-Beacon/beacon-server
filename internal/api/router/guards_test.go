// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/guards"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
)

const guardTestKey = "0123456789abcdef"

func guardRequest(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	r := httptest.NewRequest(method, path, rd)
	r.Header.Set("Authorization", "Bearer "+guardTestKey)
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// The backup route and /ws run without guard state; the public group runs with it.
func TestAdminBackupNeverCut(t *testing.T) {
	g := guards.New(guards.Options{Enforce: guards.Families, Budget: "shadow"})
	var sawState, sawDeadline bool
	probe := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tk, err := g.Admit(r.Context()) // the inert ticket when there is no state
		if err != nil {
			sawState = true
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		sctx, cancel := tk.Start(r.Context())
		defer cancel()
		defer tk.Finish()
		_, sawDeadline = sctx.Deadline()
		sawState = tk.AcquireTimeout() != 0
		w.WriteHeader(http.StatusNoContent)
	})
	h := New(nil, nil, nil, Options{Auth: config.AuthConfig{APIKey: guardTestKey}, Guards: g,
		AdminRoutes: map[string]http.Handler{"/backup": probe}})

	before := g.Begun()
	if w := guardRequest(h, http.MethodGet, "/api/v1/admin/backup", ""); w.Code != http.StatusNoContent {
		t.Fatalf("backup: %d %s", w.Code, w.Body.String())
	}
	if sawState || sawDeadline {
		t.Fatalf("backup ran with guard state: state=%v deadline=%v", sawState, sawDeadline)
	}
	if g.Begun() != before {
		t.Fatalf("admin request began a guard request: %d -> %d", before, g.Begun())
	}
	if w := guardRequest(h, http.MethodGet, "/api/v1/admin/config", ""); w.Code != http.StatusOK || g.Begun() != before {
		t.Fatalf("admin config: %d, begun %d -> %d", w.Code, before, g.Begun())
	}

	ws := httptest.NewRecorder()
	h.ServeHTTP(ws, httptest.NewRequest(http.MethodGet, "/ws", nil))
	if ws.Code >= 200 && ws.Code < 300 {
		t.Fatalf("/ws without upgrade headers: %d", ws.Code)
	}
	if g.Begun() != before {
		t.Fatalf("/ws began a guard request: %d -> %d", before, g.Begun())
	}

	if w := guardRequest(h, http.MethodGet, "/api/v1/info", ""); w.Code != http.StatusOK {
		t.Fatalf("info: %d", w.Code)
	}
	if g.Begun() != before+1 {
		t.Fatalf("public request: begun %d -> %d, want +1", before, g.Begun())
	}
}

// The request metric is labelled with chi's route pattern, never the raw path.
func TestGuardRouteLabelIsPattern(t *testing.T) {
	g := guards.New(guards.Options{Budget: "shadow"})
	h := New(nil, nil, nil, Options{Guards: g})
	const raw = "not-a-node-id-7f3a"
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/nodes/"+raw, nil))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status %d", w.Code)
	}
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	scrape := rec.Body.String()
	if strings.Contains(scrape, raw) {
		t.Fatal("raw path leaked into a metric label")
	}
	if !strings.Contains(scrape, `beacon_http_request_seconds_count{family="other",route="/api/v1/nodes/{nodeId}"}`) {
		var lines []string
		for _, l := range strings.Split(scrape, "\n") {
			if strings.HasPrefix(l, "beacon_http_request_seconds_count") {
				lines = append(lines, l)
			}
		}
		t.Fatalf("no nodes pattern series:\n%s", strings.Join(lines, "\n"))
	}
}

func TestAdminConfigGuardsUpdate(t *testing.T) {
	g := guards.New(guards.Options{Budget: "shadow"})
	h := New(nil, nil, nil, Options{Auth: config.AuthConfig{APIKey: guardTestKey}, Guards: g,
		APIPlanLayout: "custom_all", CORS: config.CORSConfig{AllowedOrigins: []string{"https://before.test"}}})

	w := guardRequest(h, http.MethodGet, "/api/v1/admin/config", "")
	var body map[string]any
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &body) != nil {
		t.Fatalf("GET: %d %s", w.Code, w.Body.String())
	}
	want := map[string]any{"enforce": []any{}, "observe_only": false, "budget": "shadow", "api_plan_layout": "custom_all"}
	if !reflect.DeepEqual(body["guards"], want) {
		t.Fatalf("guards = %#v, want %#v", body["guards"], want)
	}

	w = guardRequest(h, http.MethodPut, "/api/v1/admin/config", `{"guards":{"enforce":["stats","routes"]}}`)
	var res api.UpdateAdminConfigResponse
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &res) != nil || res.Persisted || res.RequiresRestart {
		t.Fatalf("PUT: %d %s", w.Code, w.Body.String())
	}
	if res.Config.Guards == nil || !slices.Equal(res.Config.Guards.Enforce, []string{"routes", "stats"}) {
		t.Fatalf("returned guards %+v", res.Config.Guards)
	}
	if !slices.Equal(g.Enforce(), []guards.Family{guards.Routes, guards.Stats}) {
		t.Fatalf("g.Enforce() = %v", g.Enforce())
	}
	if res.Config.CORS.AllowedOrigins[0] != "https://before.test" {
		t.Fatalf("guards update changed CORS: %v", res.Config.CORS.AllowedOrigins)
	}
	snapshot := guardRequest(h, http.MethodGet, "/api/v1/admin/config", "").Body.String()

	for _, c := range []struct{ body, msg string }{
		{`{"guards":{"enforce":["bogus"]}}`, guards.ErrUnknownFamily.Error()},
		{`{}`, "cors.allowed_origins or guards.enforce is required"},
		{`{"guards":{}}`, "guards.enforce is required"},
		{`{"guards":{"enforce":null}}`, "guards.enforce is required"},
		{`{"cors":{}}`, "cors.allowed_origins is required"},
		// Mixed: one bad half applies nothing.
		{`{"cors":{"allowed_origins":["https://after.test"]},"guards":{"enforce":["bogus"]}}`, guards.ErrUnknownFamily.Error()},
		{`{"cors":{"allowed_origins":["ftp://bad"]},"guards":{"enforce":["packets_global"]}}`, "allowed_origins must"},
	} {
		w := guardRequest(h, http.MethodPut, "/api/v1/admin/config", c.body)
		if w.Code != 400 || !strings.Contains(w.Body.String(), c.msg) {
			t.Errorf("PUT %s: %d %s, want 400 %q", c.body, w.Code, w.Body.String(), c.msg)
		}
		if !slices.Equal(g.Enforce(), []guards.Family{guards.Routes, guards.Stats}) {
			t.Fatalf("PUT %s changed g.Enforce() to %v", c.body, g.Enforce())
		}
		if got := guardRequest(h, http.MethodGet, "/api/v1/admin/config", "").Body.String(); got != snapshot {
			t.Fatalf("PUT %s changed the config view:\n%s\nwas\n%s", c.body, got, snapshot)
		}
	}

	// Both halves valid: both apply. An empty list means every family observes.
	w = guardRequest(h, http.MethodPut, "/api/v1/admin/config", `{"cors":{"allowed_origins":["https://after.test"]},"guards":{"enforce":[]}}`)
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &res) != nil {
		t.Fatalf("mixed PUT: %d %s", w.Code, w.Body.String())
	}
	if len(g.Enforce()) != 0 || res.Config.Guards == nil || res.Config.Guards.Enforce == nil || len(res.Config.Guards.Enforce) != 0 ||
		res.Config.CORS.AllowedOrigins[0] != "https://after.test" {
		t.Fatalf("mixed PUT applied %v / %+v", g.Enforce(), res.Config)
	}

	// Without a bound guard, a guards update is refused.
	plain := New(nil, nil, nil, Options{Auth: config.AuthConfig{APIKey: guardTestKey}})
	if w := guardRequest(plain, http.MethodPut, "/api/v1/admin/config", `{"guards":{"enforce":["routes"]}}`); w.Code != 400 ||
		!strings.Contains(w.Body.String(), "guards are not configured") {
		t.Fatalf("unbound guards PUT: %d %s", w.Code, w.Body.String())
	}
	if w := guardRequest(plain, http.MethodGet, "/api/v1/admin/config", ""); strings.Contains(w.Body.String(), `"guards"`) {
		t.Fatalf("router without guards reports them: %s", w.Body.String())
	}
}

// Under the single layout every family observes, and the view says so.
func TestAdminConfigGuardsObserveOnly(t *testing.T) {
	g := guards.New(guards.Options{Enforce: []guards.Family{guards.Routes}, ForceObserve: true, Budget: "shadow"})
	h := New(nil, nil, nil, Options{Auth: config.AuthConfig{APIKey: guardTestKey}, Guards: g, APIPlanLayout: "single"})
	var cfg api.AdminConfig
	if err := json.Unmarshal(guardRequest(h, http.MethodGet, "/api/v1/admin/config", "").Body.Bytes(), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Guards == nil || !cfg.Guards.ObserveOnly || cfg.Guards.APIPlanLayout != "single" || !slices.Equal(cfg.Guards.Enforce, []string{"routes"}) {
		t.Fatalf("guards view %+v", cfg.Guards)
	}
}

// Concurrent updates serialize: the reported enforce list always matches the guard's.
func TestAdminConfigGuardsConcurrentUpdates(t *testing.T) {
	g := guards.New(guards.Options{Budget: "shadow"})
	h := New(nil, nil, nil, Options{Auth: config.AuthConfig{APIKey: guardTestKey}, Guards: g, APIPlanLayout: "split"})
	bodies := []string{
		`{"guards":{"enforce":["routes"]}}`,
		`{"guards":{"enforce":["stats","packets_global"]}}`,
		`{"cors":{"allowed_origins":["https://a.test"]},"guards":{"enforce":[]}}`,
		`{"cors":{"allowed_origins":["https://b.test"]}}`,
	}
	var wg sync.WaitGroup
	for i := range 40 {
		wg.Go(func() {
			if w := guardRequest(h, http.MethodPut, "/api/v1/admin/config", bodies[i%len(bodies)]); w.Code != 200 {
				t.Errorf("PUT: %d %s", w.Code, w.Body.String())
			}
		})
		wg.Go(func() { guardRequest(h, http.MethodGet, "/api/v1/admin/config", "") })
	}
	wg.Wait()
	var cfg api.AdminConfig
	if err := json.Unmarshal(guardRequest(h, http.MethodGet, "/api/v1/admin/config", "").Body.Bytes(), &cfg); err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, f := range g.Enforce() {
		want = append(want, string(f))
	}
	if cfg.Guards == nil || !slices.Equal(cfg.Guards.Enforce, want) && !(len(want) == 0 && len(cfg.Guards.Enforce) == 0) {
		t.Fatalf("view %v, guard %v", cfg.Guards, want)
	}
}
