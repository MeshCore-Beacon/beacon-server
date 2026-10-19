// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
)

// pageServer serves 7 items in pages: limit param, cursor ← nextCursor. mutate may change page n's body; status
// may replace page n's status.
func pageServer(t *testing.T, mutate func(page int, body map[string]any), status func(page int) int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/things" {
			http.NotFound(w, r)
			return
		}
		limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
		if err != nil || limit < 1 {
			http.Error(w, `{"error":"limit"}`, http.StatusBadRequest)
			return
		}
		cursor, _ := strconv.Atoi(r.URL.Query().Get("cursor"))
		page := cursor/limit + 1
		items := []any{}
		for id := cursor + 1; id <= 7 && len(items) < limit; id++ {
			items = append(items, map[string]any{"id": id, "name": "n" + strconv.Itoa(id), "group": (id + 1) / 2})
		}
		last := cursor + len(items)
		body := map[string]any{"items": items, "hasMore": last < 7}
		if last < 7 {
			body["nextCursor"] = last
		}
		if mutate != nil {
			mutate(page, body)
		}
		code := http.StatusOK
		if status != nil {
			code = status(page)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func thingsSpec(allowed []string, key []string) *planguard.StoreDiffSpec {
	return &planguard.StoreDiffSpec{Cases: []planguard.StoreCase{{
		ID: "things", Path: "/api/v1/things", WebLimit: 3,
		Follow:  []planguard.Follow{{Param: "cursor", From: "nextCursor"}},
		Allowed: allowed, Key: key,
	}}}
}

func runStoreDiff(t *testing.T, oldSrv, newSrv *httptest.Server, spec *planguard.StoreDiffSpec, limits ...string) *planguard.StoreDiffReport {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rep, err := planguard.RunStoreDiff(ctx, oldSrv.Client(), oldSrv.URL, newSrv.URL, spec, limits)
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func TestStoreDiffComparesPageJSON(t *testing.T) {
	old := pageServer(t, nil, nil)

	t.Run("identical", func(t *testing.T) {
		rep := runStoreDiff(t, old, pageServer(t, nil, nil), thingsSpec(nil, nil), "1", "2", "3", "web")
		wantPages := map[int]int{1: 7, 2: 4, 3: 3}
		if len(rep.Cases) != 4 {
			t.Fatalf("results = %+v, want 4", rep.Cases)
		}
		for i, c := range rep.Cases {
			if c.Verdict != planguard.DiffIdentical {
				t.Errorf("limit %d: %s (%s), want identical", c.Limit, c.Verdict, c.Detail)
			}
			if i == 3 && c.Limit != 3 {
				t.Errorf("web limit = %d, want the case's web_limit 3", c.Limit)
			}
			if c.Pages != wantPages[c.Limit] {
				t.Errorf("limit %d: %d pages, want %d", c.Limit, c.Pages, wantPages[c.Limit])
			}
		}
		if len(rep.Failed()) != 0 || !strings.Contains(rep.Markdown(), "| things | 1 | 7 | identical |") {
			t.Errorf("report:\n%s", rep.Markdown())
		}
	})

	for _, tc := range []struct {
		name   string
		mutate func(page int, body map[string]any)
		status func(page int) int
		detail string
	}{
		{"hasMore flips on page 3", func(page int, body map[string]any) {
			if page == 3 {
				body["hasMore"] = false
			}
		}, nil, "page 3: $.hasMore: old true, new false"},
		{"item name changes on page 2", func(page int, body map[string]any) {
			if page == 2 {
				body["items"].([]any)[0].(map[string]any)["name"] = "renamed"
			}
		}, nil, `page 2: $.items[0].name: old "n2", new "renamed"`},
		{"a field is added", func(page int, body map[string]any) {
			body["items"].([]any)[0].(map[string]any)["extra"] = 1
		}, nil, "page 1: $.items[0].extra: old (absent), new 1"},
		{"a field is removed", func(page int, body map[string]any) {
			delete(body, "nextCursor")
		}, nil, "page 1: $.nextCursor: old 1, new (absent)"},
		{"a number changes type", func(page int, body map[string]any) {
			if page == 4 {
				body["nextCursor"] = "4"
			}
		}, nil, `page 4: $.nextCursor: old 4, new "4"`},
		{"new answers 500", nil, func(page int) int {
			if page == 2 {
				return http.StatusInternalServerError
			}
			return http.StatusOK
		}, "page 2: status: old 200, new 500"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rep := runStoreDiff(t, old, pageServer(t, tc.mutate, tc.status), thingsSpec(nil, nil), "1")
			c := rep.Cases[0]
			if c.Verdict != planguard.DiffDifferent || c.Detail != tc.detail {
				t.Errorf("got %s %q, want different %q", c.Verdict, c.Detail, tc.detail)
			}
			if len(rep.Failed()) != 1 {
				t.Errorf("Failed() = %d, want 1", len(rep.Failed()))
			}
		})
	}

	t.Run("equal non-200 answers end the walk", func(t *testing.T) {
		status := func(page int) int {
			if page == 2 {
				return http.StatusServiceUnavailable
			}
			return http.StatusOK
		}
		rep := runStoreDiff(t, pageServer(t, nil, status), pageServer(t, nil, status), thingsSpec(nil, nil), "1")
		if c := rep.Cases[0]; c.Verdict != planguard.DiffIdentical || c.Pages != 2 {
			t.Errorf("got %+v, want identical over 2 pages", c)
		}
	})

	t.Run("additive_keys allows a declared added field", func(t *testing.T) {
		add := func(page int, body map[string]any) { body["items"].([]any)[0].(map[string]any)["extra"] = 1 }
		rep := runStoreDiff(t, old, pageServer(t, add, nil), thingsSpec([]string{planguard.ClassAdditiveKeys}, nil), "3")
		c := rep.Cases[0]
		if c.Verdict != planguard.DiffAllowed || len(c.Classes) != 1 || c.Classes[0] != "additive_keys(3)" ||
			!strings.Contains(c.Detail, "$.items[0].extra") {
			t.Errorf("got %+v, want allowed additive_keys(3)", c)
		}
		// A removed field is still a difference.
		drop := func(page int, body map[string]any) { delete(body["items"].([]any)[0].(map[string]any), "name") }
		rep = runStoreDiff(t, old, pageServer(t, drop, nil), thingsSpec([]string{planguard.ClassAdditiveKeys}, nil), "3")
		if c := rep.Cases[0]; c.Verdict != planguard.DiffDifferent {
			t.Errorf("removed field: got %+v, want different", c)
		}
	})

	t.Run("tie_order allows a reorder within equal keys only", func(t *testing.T) {
		swap := func(i, j int) func(page int, body map[string]any) {
			return func(page int, body map[string]any) {
				if items := body["items"].([]any); page == 1 && len(items) > j {
					items[i], items[j] = items[j], items[i]
				}
			}
		}
		spec := thingsSpec([]string{planguard.ClassTieOrder}, []string{"group"})
		// Items 1 and 2 share group 1.
		rep := runStoreDiff(t, old, pageServer(t, swap(0, 1), nil), spec, "3")
		if c := rep.Cases[0]; c.Verdict != planguard.DiffAllowed || len(c.Classes) != 1 || c.Classes[0] != "tie_order(2)" {
			t.Errorf("tie swap: got %+v, want allowed tie_order(2)", c)
		}
		// Items 2 and 3 are in different groups.
		rep = runStoreDiff(t, old, pageServer(t, swap(1, 2), nil), spec, "3")
		if c := rep.Cases[0]; c.Verdict != planguard.DiffDifferent || !strings.HasPrefix(c.Detail, "page 1: $.items[1]") {
			t.Errorf("cross-key swap: got %+v, want different at $.items[1]", c)
		}
		// Undeclared, the tie swap is a difference.
		rep = runStoreDiff(t, old, pageServer(t, swap(0, 1), nil), thingsSpec(nil, nil), "3")
		if c := rep.Cases[0]; c.Verdict != planguard.DiffDifferent {
			t.Errorf("undeclared tie swap: got %+v, want different", c)
		}
	})
}

func TestStoreDiffStopsOnStuckCursor(t *testing.T) {
	if d, ok := t.Deadline(); ok && time.Until(d) < 10*time.Second {
		t.Skip("too close to the test deadline")
	}
	stuck := func(page int, body map[string]any) {
		body["hasMore"] = true
		body["nextCursor"] = 1
	}
	old, newSrv := pageServer(t, stuck, nil), pageServer(t, stuck, nil)
	spec := thingsSpec(nil, nil)
	spec.Cases[0].Pages = 1000
	done := make(chan *planguard.StoreDiffReport, 1)
	go func() { done <- runStoreDiff(t, old, newSrv, spec, "1") }()
	select {
	case rep := <-done:
		c := rep.Cases[0]
		if c.Verdict != planguard.DiffDifferent || c.Stuck != "cursor did not advance at page 2" || c.Pages != 2 {
			t.Errorf("got %+v, want different, stuck at page 2", c)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the walk did not stop")
	}
}

func TestStoreDiffSpecValidation(t *testing.T) {
	for _, tc := range []struct{ yaml, err string }{
		{"cases: []\n", "no cases"},
		{"cases:\n- id: a\n  path: /x\n", `path "/x"`},
		{"cases:\n- id: a\n  path: /api/v1/x?limit=1\n", "without a query"},
		{"cases:\n- id: a\n  path: /api/v1/x\n  bogus: 1\n", "field bogus not found"},
		{"cases:\n- id: a\n  path: /api/v1/x\n  query: {limit: \"5\"}\n", "query limit is set by the walk"},
		{"cases:\n- id: a\n  path: /api/v1/x\n  follow: [{param: cursor, from: prevCursor}]\n", "want nextCursor, nextPageCursor or last.<field>"},
		{"cases:\n- id: a\n  path: /api/v1/x\n  allowed: [tie_order]\n", "tie_order needs key"},
		{"cases:\n- id: a\n  path: /api/v1/x\n  allowed: [saturated_suffix]\n", "want tie_order or additive_keys"},
		{"cases:\n- id: a\n  path: /api/v1/x/{id}\n", "vars does not define it"},
		{"cases:\n- id: a\n  path: /api/v1/x\n- id: a\n  path: /api/v1/y\n", "case id a is used twice"},
	} {
		if _, err := planguard.ParseStoreDiffSpec([]byte(tc.yaml), "t.yaml"); err == nil || !strings.Contains(err.Error(), tc.err) {
			t.Errorf("%q: error %v, want %q", tc.yaml, err, tc.err)
		}
	}
}

// A path var is read from both servers; a missing value skips the case.
func TestStoreDiffPathVars(t *testing.T) {
	srv := func(ids ...int) *httptest.Server {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch {
			case r.URL.Path == "/api/v1/channels":
				items := []any{}
				for _, id := range ids {
					items = append(items, map[string]any{"id": id})
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "hasMore": false})
			case strings.HasPrefix(r.URL.Path, "/api/v1/channels/"):
				_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{r.URL.Path}, "hasMore": false})
			default:
				http.NotFound(w, r)
			}
		}))
		t.Cleanup(s.Close)
		return s
	}
	spec := &planguard.StoreDiffSpec{Cases: []planguard.StoreCase{{
		ID: "channel_messages", Path: "/api/v1/channels/{channel}/messages",
		Vars: map[string]planguard.StoreVar{"channel": {Path: "/api/v1/channels", Query: map[string]string{"limit": "1"}, From: "first.id"}},
	}}}
	if c := runStoreDiff(t, srv(12), srv(12), spec, "2").Cases[0]; c.Verdict != planguard.DiffIdentical {
		t.Errorf("same var: %+v", c)
	}
	if c := runStoreDiff(t, srv(12), srv(13), spec, "2").Cases[0]; c.Verdict != planguard.DiffDifferent || c.Detail != "vars channel: old 12, new 13" {
		t.Errorf("different var: %+v", c)
	}
	rep := runStoreDiff(t, srv(), srv(), spec, "2")
	if c := rep.Cases[0]; c.Verdict != planguard.DiffSkip || len(rep.Skipped()) != 1 {
		t.Errorf("no value: %+v", c)
	}
}

// The repo's storediff case files load, and every query, follow and var param is a query param of its handler in
// the generated Swagger.
func TestRepoStoreDiffCasesLoad(t *testing.T) {
	spec, err := planguard.LoadStoreDiffSpec("../../db/planguard/storediff/smoke.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Cases) < 7 {
		t.Errorf("smoke.yaml has %d cases, want at least 7", len(spec.Cases))
	}
	raw, err := os.ReadFile("../../docs/swagger.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		BasePath string `json:"basePath"`
		Paths    map[string]struct {
			Get *struct {
				Parameters []struct{ Name, In string } `json:"parameters"`
			} `json:"get"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	placeholder := regexp.MustCompile(`\{[^}]+\}`)
	params := func(path string) map[string]bool {
		want := placeholder.ReplaceAllString(strings.TrimPrefix(path, doc.BasePath), "{}")
		for p, item := range doc.Paths {
			if placeholder.ReplaceAllString(p, "{}") == want && item.Get != nil {
				out := map[string]bool{}
				for _, prm := range item.Get.Parameters {
					if prm.In == "query" {
						out[prm.Name] = true
					}
				}
				return out
			}
		}
		t.Errorf("%s: no GET in docs/swagger.json", path)
		return nil
	}
	for _, c := range spec.Cases {
		names := []string{"limit"}
		for k := range c.Query {
			names = append(names, k)
		}
		for _, f := range c.Follow {
			names = append(names, f.Param)
		}
		known := params(c.Path)
		for _, n := range names {
			if known != nil && !known[n] {
				t.Errorf("%s: %s is not a query param of GET %s", c.ID, n, c.Path)
			}
		}
		for name, v := range c.Vars {
			known := params(v.Path)
			for k := range v.Query {
				if known != nil && !known[k] {
					t.Errorf("%s: vars %s: %s is not a query param of GET %s", c.ID, name, k, v.Path)
				}
			}
		}
	}
}

// hasMore without a cursor on both servers ends the walk with a note; the pages still compare.
func TestStoreDiffStopsWithoutCursor(t *testing.T) {
	drop := func(page int, body map[string]any) {
		if page == 2 {
			delete(body, "nextCursor")
		}
	}
	rep := runStoreDiff(t, pageServer(t, drop, nil), pageServer(t, drop, nil), thingsSpec(nil, nil), "1")
	if c := rep.Cases[0]; c.Verdict != planguard.DiffIdentical || c.Pages != 2 ||
		c.Detail != "page 2: hasMore, but cursor ← nextCursor is absent on both; the walk stops" {
		t.Errorf("got %+v", c)
	}
}
