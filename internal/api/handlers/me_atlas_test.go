// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	mw "github.com/MeshCore-Beacon/beacon-server/internal/api/middleware"
)

type fakeAtlas struct {
	rev       int64
	rng       string
	pins      []api.StoredPin
	observers map[uuid.UUID][]byte
	err       error
	// other is another device saving right after the next committed PUT.
	other bool
}

func (f *fakeAtlas) UserAtlas(context.Context, uuid.UUID) (api.Atlas, error) {
	if f.err != nil {
		return api.Atlas{}, f.err
	}
	rng := f.rng
	if rng == "" {
		rng = "24h"
	}
	return api.ResolveAtlas(rng, f.rev, f.pins, nil, nil), nil
}

func (f *fakeAtlas) ObserverKeysByIDs(context.Context, []uuid.UUID) (map[uuid.UUID][]byte, error) {
	return f.observers, nil
}

func (f *fakeAtlas) ReplaceUserAtlas(_ context.Context, _ uuid.UUID, base int64, rng string, pins []api.StoredPin) (api.Atlas, bool, error) {
	if f.err != nil {
		return api.Atlas{}, false, f.err
	}
	if base != f.rev {
		return api.Atlas{}, false, nil
	}
	f.rev++
	f.rng, f.pins = rng, pins
	doc := api.ResolveAtlas(rng, f.rev, pins, nil, nil)
	if f.other {
		f.rev++
	}
	return doc, true, nil
}

func atlasServer(svc AccountsService, a AtlasStore, opts AuthOptions) chi.Router {
	r := chi.NewRouter()
	r.Use(mw.UserAuth(svc, SessionCookie))
	r.Mount("/me", MeRouter(svc, opts, AtlasRoutes(a)))
	return r
}

const keyA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestMeAtlas(t *testing.T) {
	a := &fakeAtlas{}
	h := atlasServer(&fakeAccounts{}, a, accountsOpts())
	c := []reqOpt{cookie("web-token"), csrf}

	if w := call(h, "GET", "/me/atlas", ""); w.Code != 401 || errCode(t, w) != "not_signed_in" {
		t.Fatalf("signed out: %d %s", w.Code, w.Body)
	}
	w := call(h, "GET", "/me/atlas", "", c...)
	b := jsonBody(t, w)
	if w.Code != 200 || b["revision"] != float64(0) || b["range"] != "24h" || b["limit"] != float64(12) || len(b["pins"].([]any)) != 0 {
		t.Fatalf("empty: %d %s", w.Code, w.Body)
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("answer is cacheable")
	}

	put := `{"baseRevision":0,"range":"3d","pins":[{"kind":"node","publicKey":"` + keyA + `","name":"Hilltop","tiles":["airtime","battery","types","noise"],"view":"heard"}]}`
	w = call(h, "PUT", "/me/atlas", put, c...)
	b = jsonBody(t, w)
	pin := b["pins"].([]any)[0].(map[string]any)
	if w.Code != 200 || b["revision"] != float64(1) || b["range"] != "3d" || pin["kind"] != "node" || pin["publicKey"] != keyA ||
		pin["id"] != nil || pin["observerId"] != nil || pin["view"] != "heard" {
		t.Fatalf("put: %d %s", w.Code, w.Body)
	}
	if _, has := pin["observerPublicKey"]; has {
		t.Fatalf("node pin has an observer key: %s", w.Body)
	}

	// Stale revision: 409 with the stored Atlas at the top level under current, store unchanged.
	w = call(h, "PUT", "/me/atlas", `{"baseRevision":0,"range":"24h","pins":[]}`, c...)
	b = jsonBody(t, w)
	cur, _ := b["current"].(map[string]any)
	if w.Code != 409 || errCode(t, w) != "atlas_conflict" || cur["revision"] != float64(1) || cur["range"] != "3d" || len(cur["pins"].([]any)) != 1 {
		t.Fatalf("conflict: %d %s", w.Code, w.Body)
	}
	if a.rev != 1 {
		t.Fatal("conflict changed the Atlas")
	}

	// Validation names the pin; a bad request changes nothing.
	w = call(h, "PUT", "/me/atlas", `{"baseRevision":1,"range":"24h","pins":[{"kind":"node","publicKey":"zz","name":"x"}]}`, c...)
	var e map[string]APIError
	_ = jsonUnmarshal(w.Body.Bytes(), &e)
	if w.Code != 422 || e["error"].Code != "invalid_input" || !strings.Contains(e["error"].Message, "pin 1") {
		t.Fatalf("invalid: %d %s", w.Code, w.Body)
	}

	// Observer by id resolves through the store; an unknown id is 422.
	oid := uuid.New()
	a.observers = map[uuid.UUID][]byte{oid: mustKey(t, keyA)}
	w = call(h, "PUT", "/me/atlas", fmt.Sprintf(`{"baseRevision":1,"range":"24h","pins":[{"kind":"observer","observerId":%q,"name":"North"}]}`, oid), c...)
	pin = jsonBody(t, w)["pins"].([]any)[0].(map[string]any)
	if w.Code != 200 || pin["kind"] != "observer" || pin["observerPublicKey"] != keyA {
		t.Fatalf("observer put: %d %s", w.Code, w.Body)
	}
	w = call(h, "PUT", "/me/atlas", fmt.Sprintf(`{"baseRevision":2,"range":"24h","pins":[{"kind":"observer","observerId":%q}]}`, uuid.New()), c...)
	if w.Code != 422 {
		t.Fatalf("unknown observer: %d %s", w.Code, w.Body)
	}

	// A full Atlas with long names is bigger than the 4 KiB the other routes allow.
	var pins []string
	for i := 0; i < 12; i++ {
		pins = append(pins, fmt.Sprintf(`{"kind":"node","publicKey":"%064x","name":%q,"tiles":["airtime","battery","uptime","types"],"view":"heard"}`, i+1, strings.Repeat("é", 160)))
	}
	big := `{"baseRevision":2,"range":"24h","pins":[` + strings.Join(pins, ",") + `]}`
	if len(big) < 4096 {
		t.Fatal("test body is not big")
	}
	if w := call(h, "PUT", "/me/atlas", big, c...); w.Code != 200 {
		t.Fatalf("big: %d %s", w.Code, w.Body)
	}
	if w := call(h, "PUT", "/me/atlas", `{"baseRevision":3,"range":"24h","pins":[],"x":1}`, c...); w.Code != 400 {
		t.Fatalf("unknown field: %d", w.Code)
	}
}

func TestMeAtlasGuards(t *testing.T) {
	a := &fakeAtlas{}
	body := `{"baseRevision":0,"range":"24h","pins":[]}`
	h := atlasServer(&fakeAccounts{}, a, accountsOpts())
	if w := call(h, "PUT", "/me/atlas", body, cookie("web-token")); w.Code != 403 {
		t.Fatalf("no csrf header: %d", w.Code)
	}
	if w := call(h, "PUT", "/me/atlas", body); w.Code != 401 {
		t.Fatalf("signed out: %d", w.Code)
	}
	ro := accountsOpts()
	ro.ReadOnly = true
	h = atlasServer(&fakeAccounts{}, a, ro)
	if w := call(h, "PUT", "/me/atlas", body, cookie("web-token"), csrf); w.Code != 503 {
		t.Fatalf("read-only write: %d", w.Code)
	}
	if w := call(h, "GET", "/me/atlas", "", cookie("web-token")); w.Code != 200 {
		t.Fatalf("read-only read: %d", w.Code)
	}
	off := accountsOpts()
	off.Enabled = false
	h = atlasServer(&fakeAccounts{}, a, off)
	if w := call(h, "GET", "/me/atlas", "", cookie("web-token")); w.Code != 404 || errCode(t, w) != "accounts_disabled" {
		t.Fatalf("off: %d", w.Code)
	}
	a.err = errors.New("db down")
	h = atlasServer(&fakeAccounts{}, a, accountsOpts())
	if w := call(h, "GET", "/me/atlas", "", cookie("web-token")); w.Code != 500 || strings.Contains(w.Body.String(), "db down") {
		t.Fatalf("store error: %d %s", w.Code, w.Body)
	}
}

func mustKey(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

// The success answer is the document the PUT committed, not a later read another device's save may have changed.
func TestMeAtlasPutAnswersWhatItCommitted(t *testing.T) {
	a := &fakeAtlas{other: true}
	h := atlasServer(&fakeAccounts{}, a, accountsOpts())
	w := call(h, "PUT", "/me/atlas", `{"baseRevision":0,"range":"3d","pins":[]}`, cookie("web-token"), csrf)
	b := jsonBody(t, w)
	if w.Code != 200 || b["revision"] != float64(1) || b["range"] != "3d" {
		t.Fatalf("put: %d %s", w.Code, w.Body)
	}
}
