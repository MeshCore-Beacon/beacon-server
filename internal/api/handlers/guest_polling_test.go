// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
)

type fakeGuestPolling struct {
	got    polling.Caller
	policy polling.Policy
	err    error
}

func (f *fakeGuestPolling) Policy(_ context.Context, c polling.Caller) (polling.Policy, error) {
	f.got = c
	return f.policy, f.err
}

func (f *fakeGuestPolling) Owners(_ context.Context, c polling.Caller) (polling.OwnersAnswer, error) {
	f.got = c
	return polling.OwnersAnswer{Nodes: map[string]polling.OwnerEntry{}, Policy: f.policy}, f.err
}

func guestGet(h http.Handler, target string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func TestPolicyRoute(t *testing.T) {
	obs := strings.Repeat("ab", 32)
	svc := &fakeGuestPolling{policy: polling.DisabledPolicy()}
	svc.policy.ReadingFormat = "telemetry"
	rec := guestGet(GuestPollingRouter(svc), "/policy?plugin_version=0.11.1&observer="+obs+"&client=meshcore.observer_fw&client_version=1.18.0&extra=1")
	if rec.Code != 200 || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("got %d %v", rec.Code, rec.Header())
	}
	if rec.Header().Get("Content-Length") == "" || rec.Header().Get("Content-Encoding") != "" || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("bad headers: %v", rec.Header())
	}
	want := polling.Caller{Observer: mustHex(t, obs), Client: "meshcore.observer_fw", ClientVersion: "1.18.0"}
	if !reflect.DeepEqual(svc.got, want) {
		t.Fatalf("caller %+v, want %+v", svc.got, want)
	}
	if !strings.Contains(rec.Body.String(), `"reading_format":"telemetry"`) {
		t.Fatalf("body %s", rec.Body)
	}
}

func TestPolicyRouteUpperCaseObserverAndCaps(t *testing.T) {
	obs := strings.Repeat("AB", 32)
	svc := &fakeGuestPolling{policy: polling.DisabledPolicy()}
	guestGet(GuestPollingRouter(svc), "/policy?observer="+obs+"&client="+strings.Repeat("c", 100)+"&client_version="+strings.Repeat("v", 100))
	if len(svc.got.Observer) != 32 || len(svc.got.Client) != 32 || len(svc.got.ClientVersion) != 23 {
		t.Fatalf("caller %+v", svc.got)
	}
}

func TestPolicyRouteNilService(t *testing.T) {
	rec := guestGet(GuestPollingRouter(nil), "/policy")
	want, _ := json.Marshal(polling.DisabledPolicy())
	if rec.Code != 200 || rec.Body.String() != string(want) {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
}

func TestPolicyRouteBadObserver(t *testing.T) {
	for _, q := range []string{"", "?observer=xyz", "?observer=" + strings.Repeat("a", 63), "?observer=" + strings.Repeat("a", 65), "?observer=" + strings.Repeat("g", 64)} {
		svc := &fakeGuestPolling{policy: polling.DisabledPolicy()}
		rec := guestGet(GuestPollingRouter(svc), "/policy"+q)
		if rec.Code != 200 || svc.got.Observer != nil {
			t.Fatalf("%q: %d observer %v", q, rec.Code, svc.got.Observer)
		}
	}
}

func TestOwnersRoute(t *testing.T) {
	svc := &fakeGuestPolling{policy: polling.DisabledPolicy()}
	rec := guestGet(GuestPollingRouter(svc), "/owners?observer="+strings.Repeat("01", 32))
	var body map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || rec.Code != 200 {
		t.Fatalf("%d %v %s", rec.Code, err, rec.Body)
	}
	if string(body["nodes"]) != "{}" || len(body["policy"]) == 0 || rec.Header().Get("Content-Length") == "" {
		t.Fatalf("body %s", rec.Body)
	}
}

func TestOwnersRouteNilService(t *testing.T) {
	rec := guestGet(GuestPollingRouter(nil), "/owners")
	want, _ := json.Marshal(polling.OwnersAnswer{Nodes: map[string]polling.OwnerEntry{}, Policy: polling.DisabledPolicy()})
	if rec.Code != 200 || rec.Body.String() != string(want) {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
}

func TestFirmwareRoutesStoreError(t *testing.T) {
	svc := &fakeGuestPolling{policy: polling.DisabledPolicy(), err: errors.New("db down")}
	for _, path := range []string{"/policy", "/owners", "/owners?observer=" + strings.Repeat("01", 32)} {
		rec := guestGet(GuestPollingRouter(svc), path)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s: got %d, want 503", path, rec.Code)
		}
		h := rec.Header()
		if h.Get("Retry-After") != "60" || h.Get("Content-Length") != strconv.Itoa(rec.Body.Len()) || h.Get("Content-Type") != "application/json" {
			t.Fatalf("%s: headers %v", path, h)
		}
		var body map[string]APIError
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"].Code != "service_unavailable" {
			t.Fatalf("%s: body %s", path, rec.Body)
		}
		if strings.Contains(rec.Body.String(), "paused") || strings.Contains(rec.Body.String(), "db down") {
			t.Fatalf("%s: body leaks a policy or the error: %s", path, rec.Body)
		}
	}
}

type failOnRead struct{ t *testing.T }

func (f *failOnRead) Read([]byte) (int, error) {
	f.t.Fatal("request body was read")
	return 0, nil
}

func TestGuestKeysNeverReadsBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/guest-keys", &failOnRead{t})
	rec := httptest.NewRecorder()
	GuestKeysGone().ServeHTTP(rec, req)
	var body map[string]APIError
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || rec.Code != 404 || body["error"].Code != "not_found" {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}

func TestSharedKeyGone(t *testing.T) {
	rec := httptest.NewRecorder()
	GuestKeysGone().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/shared-guest/key", nil))
	if rec.Code != 404 {
		t.Fatalf("got %d", rec.Code)
	}
}

func mustHex(t *testing.T, s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
