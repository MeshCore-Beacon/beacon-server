// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshmapper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
)

func TestDirectoryMissingKeyDoesNotRequest(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"country":"CA","zones":[]}`))
	}))
	defer server.Close()
	store := newZoneListMemory()
	dir := NewDirectory(store, "")
	dir.listURL = server.URL + "/get_zones.php"
	now := time.Now().UTC()
	if _, _, err := dir.List(context.Background(), "CA", now); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("missing key sent an anonymous request")
	}
	if !strings.Contains(store.rows["CA"].LastError, "unconfigured") {
		t.Fatal("missing key not reported as unconfigured")
	}
	if _, fetched, err := dir.List(context.Background(), "CA", now.Add(PollInterval)); fetched || err != nil {
		t.Fatal("missing key retried immediately")
	}
}

func TestAuthenticationStatusAndRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		status int
		want   string
	}{
		{401, "authentication"}, {403, "permission"},
	} {
		var retry time.Time
		if got := statusProblem(tc.status, http.Header{}, now, &retry); !strings.Contains(got, tc.want) {
			t.Errorf("status %d not identified: %s", tc.status, got)
		}
	}
	for _, status := range []int{429, 503} {
		var retry time.Time
		deadline := now.Add(48 * time.Hour)
		h := http.Header{"Retry-After": []string{deadline.Format(http.TimeFormat)}}
		statusProblem(status, h, now, &retry)
		if !retry.Equal(deadline) {
			t.Errorf("status %d ignored HTTP-date Retry-After", status)
		}
	}
}

// Parser and cache fixtures use localhost; authenticated requests are tested separately.
func newTestDirectory(store DirectoryStore) *Directory {
	d := NewDirectory(store, "")
	d.client.Transport = http.DefaultTransport
	return d
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Only the socket destination changes; the production credential checks see the real URL.
func routeClient(t *testing.T, client *http.Client, server string) {
	t.Helper()
	u, err := url.Parse(server)
	if err != nil {
		t.Fatal(err)
	}
	client.Transport.(*authenticatedTransport).base = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		out := r.Clone(r.Context())
		out.URL.Scheme, out.URL.Host = u.Scheme, u.Host
		return http.DefaultTransport.RoundTrip(out)
	})
}

func TestAuthenticatedClientEndpointsAndRedirects(t *testing.T) {
	const key = "operator-integration-test-secret"
	var paths []string
	redirect := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.Header.Get("X-API-Key") != key {
			t.Error("request missing configured key")
		}
		if r.URL.Query().Get("key") != "" {
			t.Error("key sent in query")
		}
		if r.Header.Get("If-None-Match") != `"saved"` {
			t.Error("conditional header lost")
		}
		if r.URL.Path == "/get_zones.php" && r.URL.RawQuery != "country=CA" {
			t.Error("country parameter changed")
		}
		if redirect != "" {
			http.Redirect(w, r, redirect, http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusNotModified)
	}))
	defer server.Close()
	client := newClient(key)
	routeClient(t, client, server.URL)
	for _, path := range []string{"get_zones.php?country=CA", "get_geojson.php", "get_scopes.php", "get_channels.php", "get_repeaters.php"} {
		host := "yow.meshmapper.net"
		if strings.HasPrefix(path, "get_zones") {
			host = "meshmapper.net"
		}
		status, _, _, err := get(context.Background(), client, "Beacon-Test", "https://"+host+"/"+path, `"saved"`, MaxBody)
		if err != nil || status != 304 {
			t.Fatalf("%s: status=%d, error=%v", path, status, err)
		}
	}
	if len(paths) != 5 {
		t.Fatal("not all five endpoints requested")
	}
	for _, destination := range []string{"https://unrelated.example/get_scopes.php", "https://other.meshmapper.net/get_scopes.php", "http://yow.meshmapper.net/get_scopes.php", "/get_scopes.php"} {
		redirect = destination
		before := len(paths)
		status, _, _, err := get(context.Background(), client, "Beacon-Test", "https://yow.meshmapper.net/get_scopes.php", `"saved"`, MaxBody)
		if err != nil || status != 302 || len(paths) != before+1 {
			t.Fatal("redirect followed")
		}
	}
}

func TestAuthenticatedClientRejectsUntrustedTargets(t *testing.T) {
	client := newClient("operator-integration-test-secret")
	calls := 0
	client.Transport.(*authenticatedTransport).base = roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, errors.New("unexpected network call")
	})
	for _, endpoint := range []string{
		"https://unrelated.example/get_scopes.php", "http://yow.meshmapper.net/get_scopes.php",
		"https://yow.meshmapper.net.evil.example/get_scopes.php", "https://evilmeshmapper.net/get_scopes.php",
		"https://user@yow.meshmapper.net/get_scopes.php", "https://yow.meshmapper.net:8443/get_scopes.php",
		"https://yow.meshmapper.net/coverage.php?key=coverage-test-key", "https://yow.meshmapper.net/other.php",
		"https://yow.meshmapper.net/%67et_scopes.php", "https://yow.meshmapper.net/get_scopes.php#fragment",
	} {
		_, _, _, err := get(context.Background(), client, "Beacon-Test", endpoint, "", MaxBody)
		if !errors.Is(err, errUntrustedEndpoint) {
			t.Errorf("untrusted endpoint accepted: %s", endpoint)
		}
	}
	if calls != 0 {
		t.Fatal("credentials could reach an untrusted target or Coverage API")
	}
}

func TestAuthenticatedClientInvalidKeyAndTransportErrors(t *testing.T) {
	for _, key := range []string{"", "bad\r\nkey", "bad\x00key", "bad key"} {
		client := newClient(key)
		client.Transport.(*authenticatedTransport).base = roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Error("unconfigured client made a network request")
			return nil, errors.New("unexpected request")
		})
		_, _, _, err := get(context.Background(), client, "Beacon-Test", ZonesURL+"?country=CA", "", MaxBody)
		if !strings.Contains(requestProblem(err), "unconfigured") {
			t.Fatal("unusable key not identified")
		}
	}
	client := newClient("reflected-test-secret")
	client.Transport.(*authenticatedTransport).base = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("reflected-test-secret")
	})
	_, _, _, err := get(context.Background(), client, "Beacon-Test", ZonesURL+"?country=CA", "", MaxBody)
	if requestProblem(err) != "request failed" {
		t.Fatal("transport diagnostics not sanitized")
	}
}

type authSite struct {
	*httptest.Server
	key    string
	status map[string]int
	after  string
	calls  map[string]int
}

func newAuthSite(t *testing.T, key string, iatas []string) *authSite {
	t.Helper()
	f := &authSite{key: key, status: map[string]int{}, calls: map[string]int{}}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls[r.URL.Path]++
		if r.Header.Get("X-API-Key") != f.key {
			t.Error("importer omitted configured key")
		}
		if r.URL.Query().Has("key") {
			t.Error("credential in URL")
		}
		w.Header().Set("ETag", `"auth-v1"`)
		w.Header().Set("Retry-After", f.after)
		status := f.status[r.URL.Path]
		if status != 0 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte("upstream reflected " + f.key))
			return
		}
		iata := strings.ToUpper(strings.Split(r.Host, ".")[0])
		switch r.URL.Path {
		case "/get_zones.php":
			country := r.URL.Query().Get("country")
			if country != "CA" && country != "US" {
				t.Error("missing or changed country")
			}
			var zones []map[string]any
			for _, code := range iatas {
				if (code == "JFK") != (country == "US") {
					continue
				}
				zones = append(zones, map[string]any{"code": code, "url": "https://" + strings.ToLower(code) + ".meshmapper.net/", "has_boundary": true})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"country": country, "zones": zones})
		case "/get_geojson.php":
			_, _ = w.Write([]byte(boundaryBody(iata, square)))
		case "/get_scopes.php":
			_, _ = w.Write(bytes.ReplaceAll(catalogue(strings.ToLower(iata)), []byte("YOW"), []byte(iata)))
		case "/get_channels.php":
			_, _ = w.Write(channelList(iata, "#"+strings.ToLower(iata)))
		default:
			t.Error("unexpected endpoint")
		}
	}))
	t.Cleanup(f.Close)
	return f
}

type authHarness struct {
	dir          *Directory
	zones        *Zones
	scopes       *Importer
	channels     *Channels
	lists        *zoneListMemory
	bounds       *zoneMemoryStore
	catalogues   *memoryStore
	channelLists *channelMemoryStore
}

func newAuthHarness(t *testing.T, site *authSite, key string, iatas []string, saved *authHarness) *authHarness {
	t.Helper()
	h := &authHarness{}
	if saved != nil {
		h.lists, h.bounds, h.catalogues, h.channelLists = saved.lists, saved.bounds, saved.catalogues, saved.channelLists
	} else {
		h.lists = newZoneListMemory()
		h.bounds = &zoneMemoryStore{iatas: iatas, rows: map[string]Boundary{}}
		h.catalogues, h.channelLists = newMemoryStore(iatas...), newChannelMemoryStore(iatas...)
	}
	h.dir = NewDirectory(h.lists, key)
	routeClient(t, h.dir.client, site.URL)
	ctx := context.Background()
	if err := h.dir.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	h.zones = NewZones(config.MeshMapperZonesConfig{Enabled: true}, h.bounds, h.dir)
	if err := h.zones.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	var err error
	h.scopes, err = New(ctx, config.MeshMapperScopesConfig{Enabled: true}, h.catalogues, h.dir, scopestore.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	h.channels, err = NewChannels(ctx, config.MeshMapperChannelsConfig{Enabled: true}, h.channelLists, h.dir, keystore.NewMapKeyStore(nil))
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *authHarness) tick(t *testing.T, n int) {
	t.Helper()
	for range n {
		for _, refresh := range []func(context.Context) error{h.zones.Refresh, h.scopes.Refresh, h.channels.Refresh} {
			if err := refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestAuthenticatedImportersDeploymentScopes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		iatas []string
	}{
		{"regional", []string{"YOW"}}, {"group", []string{"YOW", "YUL"}}, {"global", []string{"YOW", "JFK"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := tc.name + "-operator-test-key"
			f := newAuthSite(t, key, tc.iatas)
			h := newAuthHarness(t, f, key, tc.iatas, nil)
			h.tick(t, 5)
			for _, iata := range tc.iatas {
				if h.bounds.rows[iata].Feature == nil || len(h.channelLists.members[iata]) != 1 || !slices.Contains(names(h.scopes.scopes), "#"+strings.ToLower(iata)) {
					t.Fatalf("%s data not imported", iata)
				}
			}
			countries := 1
			if tc.name == "global" {
				countries = 2
			}
			if f.calls["/get_zones.php"] != countries {
				t.Fatal("country lists not shared between importers")
			}
			for _, endpoint := range []string{"/get_geojson.php", "/get_scopes.php", "/get_channels.php"} {
				if f.calls[endpoint] != len(tc.iatas) {
					t.Fatalf("%s unexpected request count", endpoint)
				}
			}
		})
	}
}

func TestAuthenticatedRefreshFailuresKeepCachedData(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      int
		after, want string
		wait        time.Duration
	}{
		{"invalid key", 401, "", "authentication", 0},
		{"insufficient regional permissions", 403, "", "permission", 0},
		{"rate limit", 429, "172800", "HTTP 429", 48 * time.Hour},
		{"unavailable", 503, "172800", "HTTP 503", 48 * time.Hour},
		{"missing key", 0, "", "unconfigured", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const key = "failure-test-secret-canary"
			var logs bytes.Buffer
			oldLogger := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
			t.Cleanup(func() { slog.SetDefault(oldLogger) })
			f := newAuthSite(t, key, []string{"YOW"})
			h := newAuthHarness(t, f, key, []string{"YOW"}, nil)
			h.tick(t, 2)
			goodBoundary := h.bounds.rows["YOW"]
			goodScope, goodChannel := h.scopes.sources[0].cache, h.channels.sources[0].cache
			goodList := h.lists.rows["CA"]
			if tc.name == "missing key" {
				h = newAuthHarness(t, f, "", []string{"YOW"}, h)
			}
			for _, path := range []string{"/get_geojson.php", "/get_scopes.php", "/get_channels.php", "/get_zones.php"} {
				f.status[path] = tc.status
			}
			f.after = tc.after
			h.zones.regions[0].b.NextAttempt = time.Time{}
			h.scopes.sources[0].cache.NextAttempt = time.Time{}
			h.channels.sources[0].cache.NextAttempt = time.Time{}
			h.tick(t, 1)
			list := h.dir.lists["CA"]
			list.fetchedAt, list.nextAttempt = time.Time{}, time.Time{}
			if _, _, err := h.dir.List(context.Background(), "CA", time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			boundary, scope, channel, savedList := h.bounds.rows["YOW"], h.scopes.sources[0].cache, h.channels.sources[0].cache, h.lists.rows["CA"]
			for _, cache := range []Cache{scope, channel,
				{LastError: boundary.LastError, AttemptedAt: boundary.AttemptedAt, NextAttempt: boundary.NextAttempt},
				{LastError: savedList.LastError, AttemptedAt: savedList.AttemptedAt, NextAttempt: savedList.NextAttempt},
			} {
				if !strings.Contains(cache.LastError, tc.want) {
					t.Fatalf("failure not recorded: %s", cache.LastError)
				}
				if tc.wait != 0 && cache.NextAttempt.Sub(cache.AttemptedAt) != tc.wait {
					t.Fatal("Retry-After not retained")
				}
			}
			if !bytes.Equal(boundary.Feature, goodBoundary.Feature) || !boundary.CheckedAt.Equal(goodBoundary.CheckedAt) || boundary.ETag != goodBoundary.ETag ||
				!bytes.Equal(scope.Payload, goodScope.Payload) || !scope.CheckedAt.Equal(goodScope.CheckedAt) || scope.ETag != goodScope.ETag ||
				!bytes.Equal(channel.Payload, goodChannel.Payload) || !channel.CheckedAt.Equal(goodChannel.CheckedAt) || channel.ETag != goodChannel.ETag ||
				!bytes.Equal(savedList.Payload, goodList.Payload) || !savedList.FetchedAt.Equal(goodList.FetchedAt) || savedList.ETag != goodList.ETag {
				t.Fatal("failed refresh replaced data or advanced freshness")
			}
			for path, calls := range f.calls {
				want := 2
				if tc.name == "missing key" {
					want = 1
				}
				if calls != want {
					t.Fatalf("%s: unexpected retries or missing-key requests: %d", path, calls)
				}
			}
			before := fmt.Sprint(f.calls)
			restarted := newAuthHarness(t, f, key, []string{"YOW"}, h)
			restarted.tick(t, 5)
			if fmt.Sprint(f.calls) != before {
				t.Fatal("restart retried before backoff elapsed")
			}
			if !restarted.lists.rows["CA"].AttemptedAt.Equal(savedList.AttemptedAt) ||
				!restarted.scopes.sources[0].cache.AttemptedAt.Equal(scope.AttemptedAt) ||
				!restarted.channels.sources[0].cache.AttemptedAt.Equal(channel.AttemptedAt) ||
				!restarted.zones.regions[0].b.AttemptedAt.Equal(boundary.AttemptedAt) {
				t.Fatal("restart attempted a refresh before persisted backoff elapsed")
			}
			if restarted.zones.regions[0].b.Feature == nil || len(restarted.channels.sources[0].entries) != 1 || len(names(restarted.scopes.scopes)) != 1 || len(restarted.dir.lists["CA"].zones) != 1 {
				t.Fatal("restart lost last successful cache")
			}
			if strings.Contains(logs.String(), key) {
				t.Fatal("key leaked to logs")
			}
		})
	}
}
