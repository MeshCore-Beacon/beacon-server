// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshmapper

import (
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"slices"
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
	Key  []byte
	Data api.MeshMapperRepeater
}

// RepeaterStore keys sources by group code, or by IATA for a region outside a group.
type RepeaterStore interface {
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
}

// NewRepeaters restores fetch state without HTTP requests. Disabled, it drops every
// imported repeater. onChange runs whenever node detail may have changed.
func NewRepeaters(ctx context.Context, cfg config.MeshMapperRepeatersConfig, store RepeaterStore, dir *Directory, onChange func(context.Context)) (*Repeaters, error) {
	r := &Repeaters{store: store, dir: dir, repeatersURL: repeatersEndpoint, interval: cfg.Interval(), onChange: onChange,
		seen: map[string]bool{}, sources: map[string]*repeaterSource{}, warned: map[string]time.Time{}, client: dir.client}
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
	saved, err := store.ListRepeaterSources(ctx)
	if err != nil {
		return nil, fmt.Errorf("restore repeater sources: %w", err)
	}
	for _, cat := range saved {
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
	case status == http.StatusNotModified:
		if s.cache.CheckedAt.IsZero() {
			update.LastError = "304 without cached list"
		}
	default:
		update.LastError = statusProblem(status, header, now, &retryAt)
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
		if status == http.StatusTooManyRequests {
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
		r.changed(ctx)
	}
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
	HexID       string   `json:"hex_id"`
	IATA        string   `json:"iata"`
	Power       *string  `json:"power"`
	Hardware    *string  `json:"hardware"`
	Antenna     *string  `json:"antenna"`
	HeightM     *float64 `json:"height_m"`
	HeightASLM  *float64 `json:"height_asl_m"`
	PowerSource *string  `json:"power_source"`
	SiteNotes   *string  `json:"site_notes"`
	IsCarpeater *int     `json:"is_carpeater"`
	Admins      []string `json:"admins"`
	Scopes      []struct {
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
		out = append(out, Repeater{Key: key, Data: raw.normalize()})
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
