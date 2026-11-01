// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshmapper

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/iatadb"
)

const (
	MaxRepeaters    = 20000    // per response; a group site lists every member's
	MaxRepeaterBody = 32 << 20 // decompressed

	repeaterRetry = config.MinRepeatersRefresh

	maxAdmins      = 32
	maxScopes      = 64
	maxNameRunes   = 64
	latestUnixTime = 32503680000 // year 3000; rejects ms or garbage in seconds fields
)

var repeaterIATA = regexp.MustCompile(`^[A-Z0-9]{3,8}$`)

// Repeater is one imported repeater, keyed by its full public key.
type Repeater struct {
	Key     []byte
	Data    api.MeshMapperRepeater
	locator string
}

// RepeaterStore keys sources by group code, or by IATA for a region outside a group.
type RepeaterStore interface {
	PrepareRepeaterAccess(ctx context.Context, fingerprint string) (bool, error)
	ListKnownIATAs(ctx context.Context) ([]string, error)
	ListRepeaterSources(ctx context.Context) ([]Catalogue, error)
	// SaveRepeaterSource records a fetch; non-nil repeaters also replace the source's list.
	SaveRepeaterSource(ctx context.Context, source, url string, cache Cache, repeaters []Repeater) error
	DeleteRepeaterSource(ctx context.Context, source string) error
	// ClearRepeaters drops every imported repeater but keeps fetch timing.
	ClearRepeaters(ctx context.Context) (bool, error)
}

type repeaterRegion struct{ iata, country string }

type repeaterSource struct {
	key, url string
	cache    Cache // Payload unused: the list lives in its own table
}

// Repeaters is owned by one background task.
type Repeaters struct {
	store        RepeaterStore
	dir          *Directory
	repeatersURL func(site string) (string, bool)
	client       *http.Client
	interval     time.Duration
	onChange     func(context.Context)
	seen         map[string]bool
	regions      []repeaterRegion
	sources      map[string]*repeaterSource
	pausedUntil  time.Time            // a 429 spends the key's shared budget, so every source waits
	warned       map[string]time.Time // invalid site URL, logged once per interval
	iatas        []string
}

// NewRepeaters restores fetch state without HTTP requests. Disabled, it drops every
// imported repeater. onChange runs whenever node detail may have changed.
func NewRepeaters(ctx context.Context, cfg config.MeshMapperRepeatersConfig, store RepeaterStore, dir *Directory, onChange func(context.Context)) (*Repeaters, error) {
	r := &Repeaters{store: store, dir: dir, repeatersURL: repeatersEndpoint, interval: cfg.Interval(), onChange: onChange,
		seen: map[string]bool{}, sources: map[string]*repeaterSource{}, warned: map[string]time.Time{}, client: dir.client, iatas: slices.Clone(cfg.IATAs)}
	if !cfg.Enabled {
		cleared, err := store.ClearRepeaters(ctx)
		if err != nil {
			return nil, fmt.Errorf("clear MeshMapper repeaters: %w", err)
		}
		if cleared {
			r.changed(ctx)
		}
		return r, nil
	}
	slices.Sort(r.iatas)
	r.iatas = slices.Compact(r.iatas)
	access := sha256.Sum256([]byte(fmt.Sprintf("%x:%s", dir.accessFingerprint, strings.Join(r.iatas, ","))))
	changed, err := store.PrepareRepeaterAccess(ctx, fmt.Sprintf("%x", access))
	if err != nil {
		return nil, fmt.Errorf("prepare MeshMapper profile access: %w", err)
	}
	if changed {
		r.changed(ctx)
	}
	saved, err := store.ListRepeaterSources(ctx)
	if err != nil {
		return nil, fmt.Errorf("restore repeater sources: %w", err)
	}
	for _, cat := range saved {
		// An interrupted/failed replacement cannot safely validate a retained list.
		if cat.LastError != "" {
			cat.ETag = ""
		}
		s := &repeaterSource{key: cat.IATA, url: cat.URL, cache: cat.Cache}
		r.sources[s.key] = s
		r.log(*s, "restored", -1, 0)
	}
	iatas, err := store.ListKnownIATAs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list IATAs for MeshMapper repeaters: %w", err)
	}
	r.track(iatas)
	return r, nil
}

func (r *Repeaters) track(iatas []string) {
	if len(r.iatas) > 0 {
		iatas = r.iatas
	}
	for _, iata := range iatas {
		if r.seen[iata] {
			continue
		}
		r.seen[iata] = true
		if country := iatadb.CountryFor(iata); country != "" {
			r.regions = append(r.regions, repeaterRegion{iata, country})
		}
	}
	// Sorted so a group is always fetched from the same member's site.
	slices.SortFunc(r.regions, func(a, b repeaterRegion) int { return strings.Compare(a.iata, b.iata) })
}

// Refresh makes at most one request. The scheduler serializes calls every 15s.
func (r *Repeaters) Refresh(ctx context.Context) (err error) {
	parent := ctx
	defer func() {
		if parent.Err() != nil {
			err = nil
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()
	now := time.Now().UTC()
	if now.Before(r.pausedUntil) {
		return nil
	}
	iatas, err := r.store.ListKnownIATAs(ctx)
	if err != nil {
		return fmt.Errorf("list IATAs for MeshMapper repeaters: %w", err)
	}
	r.track(iatas)
	wanted, order, complete := map[string]string{}, []string{}, true
	for _, region := range r.regions {
		zones, fetched, err := r.dir.List(ctx, region.country, now)
		if err != nil || fetched {
			return err
		}
		if zones == nil {
			complete = false
			continue
		}
		entry, listed := zones[region.iata]
		if !listed {
			continue
		}
		endpoint, valid := r.repeatersURL(entry.url)
		if !valid {
			r.warnInvalidSite(region.iata, now)
			continue
		}
		// Any member's site lists the whole group, so one request covers every member.
		key := cmp.Or(entry.group, region.iata)
		if _, ok := wanted[key]; !ok {
			wanted[key], order = endpoint, append(order, key)
		}
	}
	// Only with every country's list in hand can a missing source mean it's gone.
	if complete {
		if err := r.retire(ctx, wanted); err != nil {
			return err
		}
	}
	for _, key := range order {
		s := r.sources[key]
		if s == nil {
			s = &repeaterSource{key: key}
			r.sources[key] = s
		}
		if s.url != wanted[key] {
			s.url, s.cache.ETag = wanted[key], ""
		}
		if now.Before(s.cache.NextAttempt) {
			continue
		}
		return r.refresh(ctx, s, now)
	}
	return nil
}

// retire drops sources no listed region maps to, e.g. a region that joined a group.
func (r *Repeaters) retire(ctx context.Context, wanted map[string]string) error {
	for key, s := range r.sources {
		if _, ok := wanted[key]; ok {
			continue
		}
		if err := r.store.DeleteRepeaterSource(ctx, key); err != nil {
			return fmt.Errorf("retire repeater source %s: %w", key, err)
		}
		delete(r.sources, key)
		r.log(*s, "retired", -1, 0)
		r.changed(ctx)
	}
	return nil
}

func (r *Repeaters) refresh(ctx context.Context, s *repeaterSource, now time.Time) error {
	update := Cache{AttemptedAt: now, NextAttempt: now.Add(r.interval)}
	etag := ""
	if !s.cache.CheckedAt.IsZero() {
		etag = s.cache.ETag
	}
	// Recorded first: an abandoned request still spent the key's budget.
	attempt := Cache{AttemptedAt: now, NextAttempt: now.Add(min(r.interval, repeaterRetry)), LastError: "no response"}
	s.cache.NextAttempt = attempt.NextAttempt
	if err := r.store.SaveRepeaterSource(ctx, s.key, s.url, attempt, nil); err != nil {
		return fmt.Errorf("persist repeater source %s: %w", s.key, err)
	}
	s.cache.AttemptedAt, s.cache.LastError = now, attempt.LastError
	status, body, header, err := get(ctx, r.client, "Beacon-MeshMapper-Repeaters/1", s.url, etag, MaxRepeaterBody)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var repeaters []Repeater
	var skipped int
	var retryAt time.Time
	switch {
	case err != nil:
		update.LastError = requestProblem(err)
	case status == http.StatusOK:
		if repeaters, skipped, err = decodeRepeaters(body); err != nil {
			update.LastError = "invalid response"
		}
		if len(r.iatas) > 0 {
			repeaters = slices.DeleteFunc(repeaters, func(row Repeater) bool { return !slices.Contains(r.iatas, row.Data.IATA) })
		}
		for i := range repeaters {
			// The URL is derived only from the validated provider endpoint and full-key record.
			if u, err := url.Parse(s.url); err == nil && trustedEndpoint(u) {
				u.Path, u.RawQuery = "/", ""
				if repeaters[i].locator != "" {
					u.RawQuery = url.Values{"repeater": {repeaters[i].locator}, "preset": {"all"}}.Encode()
				}
				repeaters[i].Data.ProfileURL = u.String()
			}
		}
	case status == http.StatusNotModified:
		if s.cache.CheckedAt.IsZero() {
			update.LastError = "304 without cached list"
		}
	default:
		update.LastError = statusProblem(status, header, now, &retryAt)
		if status == http.StatusUnauthorized || status == http.StatusForbidden {
			repeaters = []Repeater{} // revoked access must not continue serving an old profile
			s.cache.CheckedAt, s.cache.ETag = time.Time{}, ""
		}
	}
	if update.LastError == "" {
		etag := header.Get("ETag")
		if len(etag) > 256 || strings.ContainsAny(etag, "\r\n") {
			update.LastError, repeaters = "invalid ETag", nil
		} else {
			if status == http.StatusNotModified && etag == "" {
				etag = s.cache.ETag
			}
			update.ETag, update.CheckedAt = etag, now
		}
	}
	if update.LastError != "" {
		update.NextAttempt = now.Add(min(r.interval, repeaterRetry))
		if retryAt.After(update.NextAttempt) {
			update.NextAttempt = retryAt
		}
		if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable && !retryAt.IsZero() {
			r.pausedUntil = update.NextAttempt
		}
	}
	if err := r.store.SaveRepeaterSource(ctx, s.key, s.url, update, repeaters); err != nil {
		s.cache.NextAttempt = update.NextAttempt // never retry every tick during a DB outage
		s.cache.LastError = "save failed"
		r.log(*s, "checked", -1, 0)
		return fmt.Errorf("persist repeater source %s: %w", s.key, err)
	}
	if !update.CheckedAt.IsZero() {
		s.cache.CheckedAt, s.cache.ETag = update.CheckedAt, update.ETag
	}
	s.cache.AttemptedAt, s.cache.NextAttempt, s.cache.LastError = now, update.NextAttempt, update.LastError
	count := -1
	if repeaters != nil {
		count = len(repeaters)
	}
	r.changed(ctx) // fetch status and freshness can change even when the payload did not
	r.log(*s, "checked", count, skipped)
	return nil
}

func (r *Repeaters) changed(ctx context.Context) {
	if r.onChange != nil {
		r.onChange(ctx)
	}
}

func (r *Repeaters) warnInvalidSite(iata string, now time.Time) {
	if now.Before(r.warned[iata]) {
		return
	}
	r.warned[iata] = now.Add(r.interval)
	slog.Warn("MeshMapper repeaters skipped", "component", "meshmapper.repeaters", "iata", iata, "last_error", "invalid site URL")
}

// log reports counts only when a list was just imported.
func (r *Repeaters) log(s repeaterSource, action string, count, skipped int) {
	level := slog.LevelInfo
	if s.cache.LastError != "" {
		level = slog.LevelWarn
	}
	attrs := []any{"component", "meshmapper.repeaters", "source", s.key, "url", s.url,
		"checked_at", s.cache.CheckedAt, "next_attempt", s.cache.NextAttempt, "last_error", s.cache.LastError}
	if count >= 0 {
		attrs = append(attrs, "repeaters", count, "skipped", skipped)
	}
	slog.Log(context.Background(), level, "MeshMapper repeaters "+action, attrs...)
}

func repeatersEndpoint(site string) (string, bool) { return siteEndpoint(site, "get_repeaters.php") }

type rawRepeater struct {
	ID            string   `json:"id"`
	Lat           *float64 `json:"lat"`
	Lon           *float64 `json:"lon"`
	HexID         string   `json:"hex_id"`
	IATA          string   `json:"iata"`
	Power         *string  `json:"power"`
	Hardware      *string  `json:"hardware"`
	Antenna       *string  `json:"antenna"`
	HeightM       *float64 `json:"height_m"`
	HeightASLM    *float64 `json:"height_asl_m"`
	PowerSource   *string  `json:"power_source"`
	SiteNotes     *string  `json:"site_notes"`
	IsCarpeater   *int     `json:"is_carpeater"`
	Admins        []string `json:"admins"`
	Enabled       *int     `json:"enabled"`
	CreatedAt     *int64   `json:"created_at"`
	LastHeard     *int64   `json:"last_heard"`
	AdvertBytes   *int     `json:"advert_bytes"`
	Backbone      *int     `json:"backbone"`
	BackboneShare *float64 `json:"backbone_share"`
	PresetCurrent *string  `json:"preset_current"`
	Radios        []struct {
		Preset    string `json:"preset"`
		LastSeen  *int64 `json:"last_seen"`
		Witnesses *int   `json:"witnesses"`
	} `json:"radios"`
	Neighbors []struct {
		Key      string   `json:"key"`
		Resolved *int     `json:"resolved"`
		SNR      *float64 `json:"snr"`
		HeardAt  *int64   `json:"heard_at"`
	} `json:"proven_neighbours"`
	Scopes []struct {
		Scope        string  `json:"scope"`
		ObservedAt   *int64  `json:"observed_at"`
		ObservedPkts *int64  `json:"observed_pkts"`
		Proof        *string `json:"proof"`
		ReportedAt   *int64  `json:"reported_at"`
		UploadedAt   *int64  `json:"uploaded_at"`
	} `json:"scopes"`
	ScopesCheckedAt   *int64  `json:"scopes_checked_at"`
	DefaultScope      *string `json:"default_scope"`
	DefaultScopeState *string `json:"default_scope_state"`
	DefaultScopeAt    *int64  `json:"default_scope_at"`
	DefaultScopeSrc   *string `json:"default_scope_src"`
}

// decodeRepeaters rejects a malformed list but skips individual bad rows (counted),
// including legacy rows without a full key; nothing here feeds decryption or routing.
func decodeRepeaters(body []byte) (out []Repeater, skipped int, err error) {
	if len(body) > MaxRepeaterBody || !utf8.Valid(body) {
		return nil, 0, fmt.Errorf("invalid repeater list size/encoding")
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, 0, err
	}
	if rows == nil || len(rows) > MaxRepeaters {
		return nil, 0, fmt.Errorf("invalid repeater list")
	}
	out = make([]Repeater, 0, len(rows))
	seen := map[string]bool{}
	for _, row := range rows {
		var raw rawRepeater
		if json.Unmarshal(row, &raw) != nil || len(raw.HexID) != 64 {
			skipped++
			continue
		}
		key, err := hex.DecodeString(raw.HexID)
		if err != nil || seen[string(key)] {
			skipped++
			continue
		}
		seen[string(key)] = true
		repeater := Repeater{Key: key, Data: raw.normalize()}
		if lat, lon := within(raw.Lat, -90, 90), within(raw.Lon, -180, 180); lat != nil && lon != nil && (*lat != 0 || *lon != 0) {
			short := strings.ToUpper(raw.ID)
			if (len(short) != 2 && len(short) != 4 && len(short) != 6) || !strings.HasPrefix(strings.ToUpper(raw.HexID), short) {
				short = strings.ToUpper(raw.HexID[:6])
			}
			repeater.locator = fmt.Sprintf("%s,%.5f,%.5f", short, *lat, *lon)
		}
		out = append(out, repeater)
	}
	return out, skipped, nil
}

func (raw rawRepeater) normalize() api.MeshMapperRepeater {
	r := api.MeshMapperRepeater{
		Site: api.MeshMapperSite{
			Power:       text(raw.Power, 20, false),
			Hardware:    text(raw.Hardware, 80, false),
			Antenna:     text(raw.Antenna, 80, false),
			HeightM:     within(raw.HeightM, -100, 2000),
			HeightASLM:  within(raw.HeightASLM, -500, 9000),
			PowerSource: oneOf(raw.PowerSource, "mains", "solar", "battery", "poe"),
			Notes:       text(raw.SiteNotes, 200, true),
		},
		Carpeater:       raw.IsCarpeater != nil && *raw.IsCarpeater == 1,
		Admins:          []string{},
		Scopes:          []api.MeshMapperScope{},
		ScopesCheckedAt: millis(raw.ScopesCheckedAt),
		FirstSeen:       millis(raw.CreatedAt), LastHeard: millis(raw.LastHeard),
		BackboneShare: within(raw.BackboneShare, 0, 1),
	}
	if raw.Enabled != nil && *raw.Enabled >= 0 && *raw.Enabled <= 3 {
		r.Status = []string{"disabled", "active", "ambiguous", "inactive"}[*raw.Enabled]
	}
	if raw.AdvertBytes != nil && *raw.AdvertBytes >= 1 && *raw.AdvertBytes <= 3 {
		r.AdvertBytes = raw.AdvertBytes
	}
	if raw.Backbone != nil && (*raw.Backbone == 0 || *raw.Backbone == 1) {
		v := *raw.Backbone == 1
		r.Backbone = &v
	}
	if raw.PresetCurrent != nil && validPreset(*raw.PresetCurrent) {
		r.CurrentPreset = raw.PresetCurrent
	}
	for _, radio := range raw.Radios {
		if len(r.Radios) >= 16 || !validPreset(radio.Preset) || slices.ContainsFunc(r.Radios, func(v api.MeshMapperRadio) bool { return v.Preset == radio.Preset }) {
			continue
		}
		v := api.MeshMapperRadio{Preset: radio.Preset, LastSeen: millis(radio.LastSeen)}
		if radio.Witnesses != nil && *radio.Witnesses >= 0 && *radio.Witnesses <= 16 {
			v.Witnesses = radio.Witnesses
		}
		r.Radios = append(r.Radios, v)
	}
	for _, neighbor := range raw.Neighbors {
		key := strings.ToUpper(neighbor.Key)
		if len(r.Neighbors) >= 128 || (len(key) != 16 && len(key) != 64) || slices.ContainsFunc(r.Neighbors, func(v api.MeshMapperNeighbor) bool { return v.Key == key }) {
			continue
		}
		if _, err := hex.DecodeString(key); err != nil {
			continue
		}
		r.Neighbors = append(r.Neighbors, api.MeshMapperNeighbor{Key: key, Resolved: len(key) == 64 && neighbor.Resolved != nil && *neighbor.Resolved == 1, SNR: within(neighbor.SNR, -40, 40), HeardAt: millis(neighbor.HeardAt)})
	}
	if repeaterIATA.MatchString(raw.IATA) {
		r.IATA = raw.IATA
	}
	for _, name := range raw.Admins {
		if n := text(&name, maxNameRunes, false); n != nil && len(r.Admins) < maxAdmins && !slices.Contains(r.Admins, *n) {
			r.Admins = append(r.Admins, *n)
		}
	}
	for _, s := range raw.Scopes {
		name := text(&s.Scope, maxNameRunes, false)
		if name == nil || len(r.Scopes) == maxScopes ||
			slices.ContainsFunc(r.Scopes, func(c api.MeshMapperScope) bool { return c.Scope == *name }) {
			continue
		}
		scope := api.MeshMapperScope{Scope: *name, ObservedAt: millis(s.ObservedAt), Proof: oneOf(s.Proof, "multibyte", "floor"),
			ReportedAt: millis(s.ReportedAt), UploadedAt: millis(s.UploadedAt)}
		if s.ObservedPkts != nil && *s.ObservedPkts >= 0 {
			scope.ObservedPackets = s.ObservedPkts
		}
		r.Scopes = append(r.Scopes, scope)
	}
	if state := oneOf(raw.DefaultScopeState, "named", "unknown", "none"); state != nil {
		r.DefaultScope = &api.MeshMapperDefaultScope{State: *state, UpdatedAt: millis(raw.DefaultScopeAt),
			Source: oneOf(raw.DefaultScopeSrc, "advert", "reported")}
		if *state == "named" {
			r.DefaultScope.Name = text(raw.DefaultScope, maxNameRunes, false)
		}
	}
	return r
}

func validPreset(preset string) bool {
	if len(preset) > 48 {
		return false
	}
	parts := strings.Split(preset, ",")
	if len(parts) != 3 {
		return false
	}
	freq, e1 := strconv.ParseFloat(parts[0], 64)
	bw, e2 := strconv.ParseFloat(parts[1], 64)
	sf, e3 := strconv.Atoi(parts[2])
	return e1 == nil && e2 == nil && e3 == nil && freq >= 100 && freq <= 3000 && bw > 0 && bw <= 1000 && sf >= 5 && sf <= 12
}

// text trims admin-entered text and drops it when empty, over-long or carrying control
// characters (Postgres jsonb rejects NUL); multiline keeps newlines.
func text(s *string, maxRunes int, multiline bool) *string {
	if s == nil {
		return nil
	}
	v := strings.TrimSpace(*s)
	if multiline {
		v = strings.ReplaceAll(v, "\r\n", "\n")
	}
	if v == "" || utf8.RuneCountInString(v) > maxRunes ||
		strings.ContainsFunc(v, func(c rune) bool { return unicode.IsControl(c) && !(multiline && c == '\n') }) {
		return nil
	}
	return &v
}

func oneOf(s *string, allowed ...string) *string {
	if s == nil || !slices.Contains(allowed, *s) {
		return nil
	}
	return s
}

func within(v *float64, lo, hi float64) *float64 {
	if v == nil || math.IsNaN(*v) || *v < lo || *v > hi {
		return nil
	}
	return v
}

func millis(seconds *int64) *int64 {
	if seconds == nil || *seconds <= 0 || *seconds > latestUnixTime {
		return nil
	}
	ms := *seconds * 1000
	return &ms
}
