// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshmapper

import (
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

const repeaterKey = "A1B2C3D4E5F60718293A4B5C6D7E8F90A1B2C3D4E5F60718293A4B5C6D7E8F90" // the doc's key is one digit short

// From the Repeaters API doc example, with a full-length key.
const repeaterDocExample = `[{
  "id": "A1", "hex_id": "` + repeaterKey + `", "name": "Gatineau Hills",
  "lat": 45.512345, "lon": -75.912345, "last_heard": 1791234567, "created_at": 1773103950,
  "enabled": 1, "iata": "YOW", "advert_bytes": 2, "hop_bytes": 2, "multibyte_capable": 1, "time_offset": 3,
  "power": "1W", "hardware": "RAK4631", "antenna": "5.8 dBi fibreglass", "height_m": 12, "height_asl_m": 341.6,
  "power_source": "solar", "site_notes": "On the fire tower, east face",
  "backbone": 1, "backbone_share": 0.084,
  "default_scope": "yow", "default_scope_state": "named", "default_scope_at": 1791230000, "default_scope_src": "advert",
  "admins": ["MrAlders0n"],
  "proven_neighbours": [{ "key": "B7C8D9E0F1A2B3C4", "resolved": 0, "snr": 6.25, "heard_at": 1791230011 }],
  "radios": [{ "preset": "910.525,62.5,7", "last_seen": 1791234500, "witnesses": 4 }],
  "preset_current": "910.525,62.5,7",
  "scopes": [{ "scope": "yow", "observed_at": 1791200000, "observed_pkts": 37, "proof": "multibyte", "reported_at": 1791100000, "uploaded_at": null }],
  "scopes_checked_at": 1791100000, "stale_repeater_hours": 24, "single_observer_mode": 0,
  "is_observer": 0, "is_carpeater": 0
}]`

func TestDecodeRepeatersDocExample(t *testing.T) {
	got, skipped, err := decodeRepeaters([]byte(repeaterDocExample))
	if err != nil || len(got) != 1 || skipped != 0 {
		t.Fatal(got, skipped, err)
	}
	if hex.EncodeToString(got[0].Key) != strings.ToLower(repeaterKey) {
		t.Fatal("key", got[0].Key)
	}
	b, _ := json.Marshal(got[0].Data)
	var legacy map[string]json.RawMessage
	_ = json.Unmarshal(b, &legacy)
	for _, field := range []string{"status", "firstSeen", "lastHeard", "advertBytes", "backbone", "backboneShare", "currentPreset", "radios", "neighbors"} {
		delete(legacy, field)
	}
	want := `{"iata":"YOW","fetchedAt":0,"site":{"power":"1W","hardware":"RAK4631","antenna":"5.8 dBi fibreglass","heightM":12,` +
		`"heightAslM":341.6,"powerSource":"solar","notes":"On the fire tower, east face"},"carpeater":false,"admins":["MrAlders0n"],` +
		`"scopes":[{"scope":"yow","observedAt":1791200000000,"observedPackets":37,"proof":"multibyte","reportedAt":1791100000000}],` +
		`"scopesCheckedAt":1791100000000,"defaultScope":{"name":"yow","state":"named","updatedAt":1791230000000,"source":"advert"}}`
	var wantFields map[string]json.RawMessage
	_ = json.Unmarshal([]byte(want), &wantFields)
	b, _ = json.Marshal(legacy)
	wantBytes, _ := json.Marshal(wantFields)
	if string(b) != string(wantBytes) {
		t.Fatalf("got  %s\nwant %s", b, want)
	}
}

func TestDecodeRepeatersSkipsBadRows(t *testing.T) {
	key := func(c string) string { return strings.Repeat(c, 64) }
	body := `[
	  {"hex_id": "A1B2C3"},
	  {"hex_id": "` + key("Z") + `"},
	  {"hex_id": "` + key("a") + `", "is_carpeater": 1, "power_source": "nuclear", "hardware": "` + strings.Repeat("x", 81) + `",
	   "admins": ["", "ok", "ok", "bad\u0000name"], "scopes": [{"scope": ""}, {"scope": "bc", "proof": "guess", "observed_at": 1791200000000}, {"scope": "bc"}],
	   "default_scope_state": "maybe", "iata": "<b>", "site_notes": "nul\u0000here", "antenna": "  whip  ", "height_m": 1e300, "height_asl_m": -9999},
	  {"hex_id": "` + key("A") + `", "power": "dup"},
	  {"hex_id": "` + key("b") + `", "height_m": "12"},
	  {"hex_id": "` + key("c") + `", "default_scope_state": "unknown", "default_scope": "secret", "site_notes": "line one\r\nline two", "power": "1\tW"}
	]`
	got, skipped, err := decodeRepeaters([]byte(body))
	if err != nil || len(got) != 2 || skipped != 4 {
		t.Fatalf("want 2 rows and 4 skipped, got %d and %d: %v", len(got), skipped, err)
	}
	a := got[0].Data
	if !a.Carpeater || a.Site.PowerSource != nil || a.Site.Hardware != nil || a.IATA != "" || a.DefaultScope != nil ||
		!slices.Equal(a.Admins, []string{"ok"}) || len(a.Scopes) != 1 || a.Scopes[0].Proof != nil || a.Scopes[0].ObservedAt != nil {
		t.Fatalf("bad values kept: %+v", a)
	}
	if a.Site.Notes != nil || a.Site.HeightM != nil || a.Site.HeightASLM != nil {
		t.Fatalf("control characters or absurd heights kept: %+v", a.Site)
	}
	if a.Site.Antenna == nil || *a.Site.Antenna != "whip" {
		t.Fatal("text not trimmed", a.Site.Antenna)
	}
	if a.Site.Power != nil {
		t.Fatal("duplicate key replaced the first row")
	}
	c := got[1].Data
	if c.DefaultScope == nil || c.DefaultScope.State != "unknown" || c.DefaultScope.Name != nil {
		t.Fatalf("unnamed default scope: %+v", c.DefaultScope)
	}
	if c.Site.Notes == nil || *c.Site.Notes != "line one\nline two" || c.Site.Power != nil {
		t.Fatalf("notes newline or single-line tab: %+v", c.Site)
	}
	for _, bad := range []string{`null`, `{}`, `[`, `"x"`} {
		if _, _, err := decodeRepeaters([]byte(bad)); err == nil {
			t.Fatalf("%s accepted", bad)
		}
	}
	if got, _, err := decodeRepeaters([]byte(`[]`)); err != nil || got == nil || len(got) != 0 {
		t.Fatal("empty region list rejected", got, err)
	}
}

func TestDecodeRepeatersLimits(t *testing.T) {
	if _, _, err := decodeRepeaters([]byte("[" + strings.Repeat("{},", MaxRepeaters) + "{}]")); err == nil {
		t.Fatal("over MaxRepeaters accepted")
	}
	if _, _, err := decodeRepeaters(make([]byte, MaxRepeaterBody+1)); err == nil {
		t.Fatal("over MaxRepeaterBody accepted")
	}
}

type repeaterMemoryStore struct {
	access    string
	iatas     []string
	rows      map[string]sourceKeyCache
	repeaters map[string][]Repeater
	saves     int
}

func (s *repeaterMemoryStore) PrepareRepeaterAccess(ctx context.Context, fingerprint string) (bool, error) {
	if s.access == fingerprint {
		return false, nil
	}
	s.access = fingerprint
	return s.ClearRepeaters(ctx)
}

type sourceKeyCache struct {
	url   string
	cache Cache
}

func newRepeaterMemoryStore(iatas ...string) *repeaterMemoryStore {
	return &repeaterMemoryStore{iatas: iatas, rows: map[string]sourceKeyCache{}, repeaters: map[string][]Repeater{}}
}

func (s *repeaterMemoryStore) ListKnownIATAs(context.Context) ([]string, error) {
	return slices.Clone(s.iatas), nil
}

func (s *repeaterMemoryStore) ListRepeaterSources(context.Context) ([]Catalogue, error) {
	var out []Catalogue
	for key, r := range s.rows {
		out = append(out, Catalogue{IATA: key, URL: r.url, Cache: r.cache})
	}
	return out, nil
}

func (s *repeaterMemoryStore) SaveRepeaterSource(_ context.Context, key, url string, next Cache, repeaters []Repeater) error {
	s.saves++
	if repeaters != nil {
		s.repeaters[key] = repeaters
	}
	if next.CheckedAt.IsZero() {
		old := s.rows[key]
		next.CheckedAt, next.ETag = old.cache.CheckedAt, old.cache.ETag
		if old.url != url {
			next.ETag = ""
		}
	}
	s.rows[key] = sourceKeyCache{url, next}
	return nil
}

func (s *repeaterMemoryStore) DeleteRepeaterSource(_ context.Context, key string) error {
	delete(s.rows, key)
	delete(s.repeaters, key)
	return nil
}

func (s *repeaterMemoryStore) ClearRepeaters(context.Context) (bool, error) {
	cleared := len(s.repeaters) > 0
	s.repeaters = map[string][]Repeater{}
	for key, r := range s.rows {
		r.cache.CheckedAt, r.cache.ETag = time.Time{}, ""
		s.rows[key] = r
	}
	return cleared, nil
}

type repeaterZone struct{ code, group string }

type repeaterSite struct {
	*httptest.Server
	zones          []repeaterZone
	body           string
	status         int
	retryAfter     string
	etag           string
	listCalls      int
	calls          int
	gzipped        bool
	ifNoneMatch    []string
	requestedPaths []string
}

func newRepeaterSite(t *testing.T, zones ...repeaterZone) *repeaterSite {
	if len(zones) == 0 {
		zones = []repeaterZone{{code: "YOW"}}
	}
	f := &repeaterSite{status: 200, body: repeaterDocExample, zones: zones}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/get_zones.php":
			f.listCalls++
			var rows []string
			for _, z := range f.zones {
				group := "null"
				if z.group != "" {
					group = `"` + z.group + `"`
				}
				rows = append(rows, fmt.Sprintf(`{"code":%q,"url":"%s/%s/","has_boundary":true,"group":%s}`, z.code, f.URL, z.code, group))
			}
			_, _ = w.Write([]byte(`{"country":"CA","zones":[` + strings.Join(rows, ",") + `]}`))
		case strings.HasSuffix(r.URL.Path, "/get_repeaters.php"):
			f.calls++
			f.requestedPaths = append(f.requestedPaths, r.URL.Path)
			f.ifNoneMatch = append(f.ifNoneMatch, r.Header.Get("If-None-Match"))
			w.Header().Set("Retry-After", f.retryAfter)
			if f.etag != "" {
				w.Header().Set("ETag", f.etag)
			}
			if f.status != 200 {
				w.WriteHeader(f.status)
				return
			}
			if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
				_, _ = w.Write([]byte(f.body))
				return
			}
			f.gzipped = true
			w.Header().Set("Content-Encoding", "gzip")
			zw := gzip.NewWriter(w)
			_, _ = zw.Write([]byte(f.body))
			_ = zw.Close()
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	t.Cleanup(f.Close)
	return f
}

type repeaterHarness struct {
	*Repeaters
	changes int
}

func newRepeaterImporter(t *testing.T, f *repeaterSite, store *repeaterMemoryStore, lists *zoneListMemory, enabled bool) *repeaterHarness {
	t.Helper()
	dir := newTestDirectory(lists)
	dir.listURL = f.URL + "/get_zones.php"
	if err := dir.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	h := &repeaterHarness{}
	r, err := NewRepeaters(context.Background(), config.MeshMapperRepeatersConfig{Enabled: enabled}, store, dir,
		func(context.Context) { h.changes++ })
	if err != nil {
		t.Fatal(err)
	}
	r.repeatersURL = func(site string) (string, bool) { return site + "get_repeaters.php", strings.HasPrefix(site, f.URL) }
	h.Repeaters = r
	return h
}

func (h *repeaterHarness) refreshN(t *testing.T, n int) {
	t.Helper()
	for range n {
		if err := h.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func (h *repeaterHarness) due(keys ...string) {
	for _, key := range keys {
		h.sources[key].cache.NextAttempt = time.Time{}
	}
}

func TestRepeatersImportGzippedList(t *testing.T) {
	f := newRepeaterSite(t)
	store := newRepeaterMemoryStore("YOW")
	h := newRepeaterImporter(t, f, store, newZoneListMemory(), true)
	h.refreshN(t, 2)
	if !f.gzipped || len(store.repeaters["YOW"]) != 1 || store.repeaters["YOW"][0].Data.Site.Hardware == nil || h.changes != 1 {
		t.Fatal("list not imported", f.gzipped, store.repeaters, h.changes)
	}
	if c := h.sources["YOW"].cache; c.LastError != "" || c.NextAttempt.Sub(c.AttemptedAt) != 6*time.Hour {
		t.Fatal("refresh state", c)
	}
	f.body = `[]`
	h.due("YOW")
	h.refreshN(t, 1)
	if got, ok := store.repeaters["YOW"]; !ok || len(got) != 0 {
		t.Fatal("empty list did not replace the region's repeaters", got)
	}
}

func TestRepeatersGroupFetchedOnce(t *testing.T) {
	f := newRepeaterSite(t, repeaterZone{"YOW", "ONQC"}, repeaterZone{"YUL", "ONQC"}, repeaterZone{"YYZ", ""})
	store := newRepeaterMemoryStore("YUL", "YOW", "YYZ")
	h := newRepeaterImporter(t, f, store, newZoneListMemory(), true)
	h.refreshN(t, 4)
	if f.calls != 2 || !slices.Equal(f.requestedPaths, []string{"/YOW/get_repeaters.php", "/YYZ/get_repeaters.php"}) {
		t.Fatal("group not fetched once from its first member", f.requestedPaths)
	}
	if _, ok := store.repeaters["ONQC"]; !ok || len(store.repeaters) != 2 {
		t.Fatal("sources", store.repeaters)
	}

	// YYZ joining the group retires its own source once every country list is in hand.
	f.zones[2].group = "ONQC"
	h.dir.lists["CA"].fetchedAt = time.Time{}
	h.dir.lists["CA"].nextAttempt = time.Time{}
	h.refreshN(t, 2)
	if _, ok := store.rows["YYZ"]; ok || h.sources["YYZ"] != nil || store.repeaters["YYZ"] != nil {
		t.Fatal("retired source kept", store.rows)
	}
}

func TestRepeatersFailuresKeepLastGood(t *testing.T) {
	for _, tc := range []struct {
		status     int
		retryAfter string
		body       string
		want       time.Duration
	}{
		{503, "", "", time.Hour},
		{429, "60", "", time.Hour},
		{429, "7200", "", 2 * time.Hour},
		{200, "", `{"error":"nope"}`, time.Hour},
	} {
		f := newRepeaterSite(t)
		store := newRepeaterMemoryStore("YOW")
		h := newRepeaterImporter(t, f, store, newZoneListMemory(), true)
		h.refreshN(t, 2)
		f.status, f.retryAfter = tc.status, tc.retryAfter
		if tc.body != "" {
			f.body = tc.body
		}
		h.due("YOW")
		h.refreshN(t, 2)
		c := h.sources["YOW"].cache
		if f.calls != 2 || c.LastError == "" || c.NextAttempt.Sub(c.AttemptedAt) != tc.want || len(store.repeaters["YOW"]) != 1 {
			t.Fatalf("HTTP %d %q: calls=%d err=%q wait=%v kept=%d", tc.status, tc.body, f.calls, c.LastError,
				c.NextAttempt.Sub(c.AttemptedAt), len(store.repeaters["YOW"]))
		}
	}
}

func TestRepeatersRateLimitPausesEverySource(t *testing.T) {
	f := newRepeaterSite(t, repeaterZone{"YOW", ""}, repeaterZone{"YYZ", ""})
	f.status, f.retryAfter = 429, "7200"
	h := newRepeaterImporter(t, f, newRepeaterMemoryStore("YOW", "YYZ"), newZoneListMemory(), true)
	h.refreshN(t, 4)
	if f.calls != 1 || h.sources["YYZ"] != nil && !h.sources["YYZ"].cache.AttemptedAt.IsZero() {
		t.Fatal("a 429 from the shared budget didn't pause the other source", f.calls)
	}
	if h.pausedUntil.Sub(h.sources["YOW"].cache.AttemptedAt) != 2*time.Hour {
		t.Fatal("pause", h.pausedUntil)
	}
	h.pausedUntil = time.Time{}
	f.status = 503
	h.due("YOW")
	h.refreshN(t, 1)
	if h.pausedUntil.IsZero() {
		t.Fatal("503 with Retry-After did not pause the shared read budget")
	}
}

func TestRepeatersNotModified(t *testing.T) {
	f := newRepeaterSite(t)
	f.etag = `"r1"`
	store := newRepeaterMemoryStore("YOW")
	h := newRepeaterImporter(t, f, store, newZoneListMemory(), true)
	h.refreshN(t, 2)
	f.status, f.etag = 304, ""
	h.due("YOW")
	h.refreshN(t, 1)
	c := h.sources["YOW"].cache
	if !slices.Equal(f.ifNoneMatch, []string{"", `"r1"`}) || c.LastError != "" || c.ETag != `"r1"` || len(store.repeaters["YOW"]) != 1 {
		t.Fatalf("304 handling: sent=%q err=%q etag=%q", f.ifNoneMatch, c.LastError, c.ETag)
	}

	// A 304 can't stand in for a list Beacon never stored.
	h.sources["YOW"].cache.CheckedAt, h.sources["YOW"].cache.ETag = time.Time{}, `"stale"`
	h.due("YOW")
	h.refreshN(t, 1)
	if got := f.ifNoneMatch[2]; got != "" || h.sources["YOW"].cache.LastError != "304 without cached list" {
		t.Fatalf("sent %q without a list, err=%q", got, h.sources["YOW"].cache.LastError)
	}
}

func TestRepeatersRestoreWithoutRequests(t *testing.T) {
	f := newRepeaterSite(t)
	store, lists := newRepeaterMemoryStore("YOW"), newZoneListMemory()
	newRepeaterImporter(t, f, store, lists, true).refreshN(t, 2)
	newRepeaterImporter(t, f, store, lists, true).refreshN(t, 2)
	if f.listCalls != 1 || f.calls != 1 {
		t.Fatal("restart made requests", f.listCalls, f.calls)
	}
}

func TestRepeatersDisabledClearsListsKeepsTiming(t *testing.T) {
	f := newRepeaterSite(t)
	store, lists := newRepeaterMemoryStore("YOW"), newZoneListMemory()
	newRepeaterImporter(t, f, store, lists, true).refreshN(t, 2)
	h := newRepeaterImporter(t, f, store, lists, false)
	row := store.rows["YOW"]
	if len(store.repeaters) != 0 || len(h.sources) != 0 || h.changes != 1 {
		t.Fatal("disabled import kept repeaters or skipped invalidation", store.repeaters, h.changes)
	}
	if row.cache.NextAttempt.IsZero() || !row.cache.CheckedAt.IsZero() || row.cache.ETag != "" {
		t.Fatal("clear dropped timing or kept the list's ETag", row.cache)
	}

	// Re-enabled, the saved backoff still applies.
	newRepeaterImporter(t, f, store, lists, true).refreshN(t, 2)
	if f.calls != 1 {
		t.Fatal("re-enable ignored backoff", f.calls)
	}
}

func TestRepeatersRequestCutShortStillCounts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	store := newRepeaterMemoryStore()
	r, err := NewRepeaters(context.Background(), config.MeshMapperRepeatersConfig{Enabled: true}, store, newTestDirectory(newZoneListMemory()), nil)
	if err != nil {
		t.Fatal(err)
	}
	s := &repeaterSource{key: "YOW", url: server.URL}
	now := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = r.refresh(ctx, s, now)
	if got := s.cache.NextAttempt.Sub(now); got != repeaterRetry {
		t.Fatal("abandoned request retried early", got)
	}
	if row := store.rows["YOW"]; row.cache.NextAttempt.Sub(now) != repeaterRetry || row.cache.LastError != "no response" {
		t.Fatal("abandoned request not persisted", row)
	}
}
