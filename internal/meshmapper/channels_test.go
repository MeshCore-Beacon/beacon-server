// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshmapper

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
)

func channelList(iata string, names ...string) []byte {
	rows := []map[string]any{}
	for _, name := range names {
		secret, hash, _ := keystore.DeriveHashtagKey(strings.TrimPrefix(name, "#"))
		rows = append(rows, map[string]any{"name": name, "key": hex.EncodeToString(secret), "hash": hex.EncodeToString([]byte{hash}), "zones": []string{iata}})
	}
	b, _ := json.Marshal(map[string]any{"generated_at": "2026-09-30T18:00:00Z", "region": iata, "zones": []string{iata}, "count": len(rows), "channels": rows})
	return b
}

type channelMemoryStore struct {
	iatas   []string
	rows    map[sourceKey]Cache
	members map[string][][]byte
	fail    bool
}

func newChannelMemoryStore(iatas ...string) *channelMemoryStore {
	return &channelMemoryStore{iatas: iatas, rows: map[sourceKey]Cache{}, members: map[string][][]byte{}}
}

func (s *channelMemoryStore) ListKnownIATAs(context.Context) ([]string, error) {
	return slices.Clone(s.iatas), nil
}

func (s *channelMemoryStore) ListChannelCatalogues(context.Context) ([]Catalogue, error) {
	var out []Catalogue
	for key, c := range s.rows {
		out = append(out, Catalogue{IATA: key.iata, URL: key.url, Cache: c})
	}
	return out, nil
}

func (s *channelMemoryStore) SaveChannelCatalogue(_ context.Context, iata, url string, next Cache, fingerprints [][]byte) error {
	if s.fail {
		return context.DeadlineExceeded
	}
	if next.Payload != nil {
		s.members[iata] = fingerprints
	}
	old := s.rows[sourceKey{iata, url}]
	if next.Payload == nil {
		next.Payload = old.Payload
	}
	if next.CheckedAt.IsZero() {
		next.CheckedAt, next.ETag = old.CheckedAt, old.ETag
	}
	s.rows[sourceKey{iata, url}] = next
	return nil
}

func (s *channelMemoryStore) ClearChannelMembers(context.Context) error {
	s.members = map[string][][]byte{}
	return nil
}

type channelSite struct {
	*httptest.Server
	list, body          string
	status              int
	retryAfter          string
	listCalls, channels int
}

func newChannelSite(t *testing.T) *channelSite {
	f := &channelSite{status: 200, body: string(channelList("YOW", "#ottawa-mesh"))}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/get_zones.php":
			f.listCalls++
			_, _ = w.Write([]byte(f.list))
		case "/get_channels.php":
			f.channels++
			w.Header().Set("ETag", `"c1"`)
			w.Header().Set("Retry-After", f.retryAfter)
			w.WriteHeader(f.status)
			if f.status == 200 {
				_, _ = w.Write([]byte(f.body))
			}
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	f.list = `{"country":"CA","zones":[{"code":"YOW","url":"` + f.URL + `/","has_boundary":true,"group":null}]}`
	t.Cleanup(f.Close)
	return f
}

type channelHarness struct {
	c     *Channels
	keys  *keystore.MapKeyStore
	added [][]byte
}

func newChannelHarness(t *testing.T, f *channelSite, store *channelMemoryStore, lists *zoneListMemory, enabled bool) *channelHarness {
	t.Helper()
	dir := NewDirectory(lists)
	dir.listURL = f.URL + "/get_zones.php"
	if err := dir.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	h := &channelHarness{keys: keystore.NewMapKeyStore(nil)}
	c, err := NewChannels(context.Background(), config.MeshMapperChannelsConfig{Enabled: enabled}, store, dir, h.keys)
	if err != nil {
		t.Fatal(err)
	}
	c.channelsURL = func(site string) (string, bool) { return site + "get_channels.php", strings.HasPrefix(site, f.URL) }
	c.OnNewKeys(func(hashes [][]byte) { h.added = append(h.added, hashes...) })
	h.c = c
	return h
}

func (h *channelHarness) refresh(t *testing.T, n int) {
	t.Helper()
	for range n {
		if err := h.c.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestChannelsImportKeysAndMembers(t *testing.T) {
	f := newChannelSite(t)
	store := newChannelMemoryStore("YOW")
	h := newChannelHarness(t, f, store, newZoneListMemory(), true)
	h.refresh(t, 2)
	secret, hash, fingerprint := keystore.DeriveHashtagKey("ottawa-mesh")
	got := h.keys.GetKey([]byte{hash})
	if len(got) != 1 || !bytes.Equal(got[0].Key, secret) || got[0].Name != "#ottawa-mesh" || got[0].Hashtag != "ottawa-mesh" {
		t.Fatalf("key not imported: %+v", got)
	}
	if len(store.members["YOW"]) != 1 || !bytes.Equal(store.members["YOW"][0], fingerprint) {
		t.Fatal("YOW membership not saved", store.members)
	}
	if len(h.added) != 1 || h.added[0][0] != hash {
		t.Fatal("new key not reported for backfill", h.added)
	}
	if b := h.c.sources[0].cache; b.NextAttempt.Sub(b.AttemptedAt) != 24*time.Hour {
		t.Fatal("refresh interval", b.NextAttempt.Sub(b.AttemptedAt))
	}
}

func TestChannelsRejectMismatchKeepsLastGood(t *testing.T) {
	f := newChannelSite(t)
	store := newChannelMemoryStore("YOW")
	h := newChannelHarness(t, f, store, newZoneListMemory(), true)
	h.refresh(t, 2)
	good := string(channelList("YOW", "#ottawa-mesh"))
	secret, hash, _ := keystore.DeriveHashtagKey("ottawa-mesh")
	for name, body := range map[string]string{
		"planted key":   strings.Replace(good, hex.EncodeToString(secret), strings.Repeat("0", 32), 1),
		"wrong key":     strings.Replace(string(channelList("YOW", "#other")), `"#other"`, `"#planted"`, 1),
		"wrong hash":    strings.Replace(good, `"hash":"`+hex.EncodeToString([]byte{hash})+`"`, `"hash":"zz"`, 1),
		"other region":  string(channelList("YYZ", "#ottawa-mesh")),
		"bad count":     strings.Replace(good, `"count":1`, `"count":2`, 1),
		"uppercase":     string(channelList("YOW", "#Ottawa")),
		"group zones":   strings.Replace(good, `"zones":["YOW"]}`, `"zones":["YOW","YYZ"]}`, 1),
		"missing count": strings.Replace(good, `"count":1,`, ``, 1),
	} {
		f.body = body
		h.c.sources[0].cache.NextAttempt = time.Time{}
		h.refresh(t, 1)
		s := h.c.sources[0]
		if s.cache.LastError != "invalid response" || len(h.keys.GetKey([]byte{hash})) != 1 || len(store.members["YOW"]) != 1 {
			t.Fatalf("%s: accepted or dropped last good list: %q", name, s.cache.LastError)
		}
		if s.cache.NextAttempt.Sub(s.cache.AttemptedAt) != 24*time.Hour {
			t.Fatalf("%s: retried before the rate limit", name)
		}
	}
}

func TestChannelsRateLimitAndFailuresWait(t *testing.T) {
	for _, tc := range []struct {
		status     int
		retryAfter string
		want       time.Duration
	}{{503, "", 24 * time.Hour}, {429, "60", 24 * time.Hour}, {429, "172800", 48 * time.Hour}} {
		f := newChannelSite(t)
		f.status, f.retryAfter = tc.status, tc.retryAfter
		h := newChannelHarness(t, f, newChannelMemoryStore("YOW"), newZoneListMemory(), true)
		h.refresh(t, 3)
		s := h.c.sources[0]
		if f.channels != 1 || s.cache.NextAttempt.Sub(s.cache.AttemptedAt) != tc.want {
			t.Fatalf("HTTP %d Retry-After %q: calls=%d wait=%v", tc.status, tc.retryAfter, f.channels, s.cache.NextAttempt.Sub(s.cache.AttemptedAt))
		}
	}
}

func TestChannelsRestoreWithoutRequests(t *testing.T) {
	f := newChannelSite(t)
	store, lists := newChannelMemoryStore("YOW"), newZoneListMemory()
	newChannelHarness(t, f, store, lists, true).refresh(t, 2)
	restarted := newChannelHarness(t, f, store, lists, true)
	_, hash, _ := keystore.DeriveHashtagKey("ottawa-mesh")
	if len(restarted.keys.GetKey([]byte{hash})) != 1 {
		t.Fatal("saved channels not restored")
	}
	restarted.refresh(t, 2)
	if f.listCalls != 1 || f.channels != 1 {
		t.Fatal("restart made requests", f.listCalls, f.channels)
	}
}

func TestChannelsDisabledClearsMembers(t *testing.T) {
	f := newChannelSite(t)
	store := newChannelMemoryStore("YOW")
	store.members["YOW"] = [][]byte{{1}}
	h := newChannelHarness(t, f, store, newZoneListMemory(), false)
	if len(store.members) != 0 || len(h.c.sources) != 0 {
		t.Fatal("disabled import kept membership or sources")
	}
}

func TestChannelsRequestCutShortStillCounts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	store := newChannelMemoryStore("YOW")
	c, err := NewChannels(context.Background(), config.MeshMapperChannelsConfig{Enabled: true}, store, NewDirectory(newZoneListMemory()), keystore.NewMapKeyStore(nil))
	if err != nil {
		t.Fatal(err)
	}
	s := &c.sources[0]
	s.url = server.URL
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = c.refresh(ctx, s, now)
	if got := s.cache.NextAttempt.Sub(now); got != channelRetry {
		t.Fatal("abandoned request retried inside the rate limit", got)
	}
	if row := store.rows[sourceKey{"YOW", server.URL}]; row.NextAttempt.Sub(now) != channelRetry {
		t.Fatal("abandoned request not persisted", row)
	}
}
