// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package meshmapper imports published regional scope catalogues and boundaries, off the ingest path.
package meshmapper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/iatadb"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
)

const (
	MaxScopes    = 64 // per source
	MaxBody      = 64 << 10
	PollInterval = 15 * time.Second // one source per tick, at most four requests/minute

	failureRetry = config.MinScopesRefresh // get_scopes.php allows one request per region per 55m
)

// Cache stores source provenance and freshness separately from packet evidence.
type Cache struct {
	Payload                             json.RawMessage
	ETag                                string
	CheckedAt, AttemptedAt, NextAttempt time.Time
	LastError                           string
}

// Catalogue is one saved source snapshot.
type Catalogue struct {
	IATA, URL string
	Cache
}

type Store interface {
	ListKnownIATAs(context.Context) ([]string, error)
	ListScopeCatalogues(context.Context) ([]Catalogue, error)
	SaveScopeCatalogue(context.Context, string, string, Cache, []scopestore.Entry) error
}

type source struct {
	iata, country, url string
	cache              Cache
	entries            []scopestore.Entry
	generated          time.Time
}

// Importer is owned by one background task; ScopeStore synchronizes its consumers.
type Importer struct {
	store     Store
	scopes    *scopestore.ScopeStore
	manual    []scopestore.Entry
	dir       *Directory
	scopesURL func(site string) (string, bool)
	seen      map[string]bool
	sources   []source
	interval  time.Duration
	client    *http.Client
	onChange  func(context.Context)
}

// SetCacheInvalidator is wired once at startup, before the background task starts.
func (i *Importer) SetCacheInvalidator(fn func(context.Context)) { i.onChange = fn }

// New restores validated snapshots before ingestion, without making HTTP requests.
func New(ctx context.Context, cfg config.MeshMapperScopesConfig, store Store, dir *Directory, scopes *scopestore.ScopeStore, manual []scopestore.Entry) (*Importer, error) {
	i := &Importer{store: store, scopes: scopes, manual: manual, dir: dir, scopesURL: scopesEndpoint, seen: map[string]bool{},
		interval: cfg.Interval(), client: &http.Client{
			Timeout:       requestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}}
	if !cfg.Enabled {
		return i, nil
	}
	saved, err := store.ListScopeCatalogues(ctx)
	if err != nil {
		return nil, fmt.Errorf("restore scope catalogues: %w", err)
	}
	// A site that moved leaves its old row behind; the latest attempt wins.
	latest := map[string]Catalogue{}
	for _, c := range saved {
		if old, ok := latest[c.IATA]; !ok || c.AttemptedAt.After(old.AttemptedAt) {
			latest[c.IATA] = c
		}
	}
	iatas, err := store.ListKnownIATAs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list IATAs for MeshMapper scopes: %w", err)
	}
	i.track(iatas, latest)
	i.publish()
	return i, nil
}

// track adds a source for each IATA not seen before.
func (i *Importer) track(iatas []string, saved map[string]Catalogue) {
	for _, iata := range iatas {
		if i.seen[iata] {
			continue
		}
		i.seen[iata] = true
		country := iatadb.CountryFor(iata)
		if country == "" {
			continue
		}
		s := source{iata: iata, country: country}
		if c, ok := saved[iata]; ok {
			s.url, s.cache = c.URL, c.Cache
			if len(c.Payload) > 0 {
				var err error
				s.entries, s.generated, err = decode(c.Payload, iata)
				if err != nil {
					s.entries = nil
					s.cache.Payload, s.cache.ETag = nil, ""
					s.cache.LastError = "invalid saved catalogue; awaiting refresh"
				}
			}
			i.log(s, "restored")
		}
		i.sources = append(i.sources, s)
	}
}

// Refresh checks only one due source. The scheduler serializes calls every 15s.
func (i *Importer) Refresh(ctx context.Context) (err error) {
	parent := ctx
	defer func() {
		if parent.Err() != nil {
			err = nil
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()
	now := time.Now().UTC()
	iatas, err := i.store.ListKnownIATAs(ctx)
	if err != nil {
		return fmt.Errorf("list IATAs for MeshMapper scopes: %w", err)
	}
	i.track(iatas, nil)
	for n := range i.sources {
		s := &i.sources[n]
		if now.Before(s.cache.NextAttempt) {
			continue
		}
		zones, fetched, err := i.dir.List(ctx, s.country, now)
		if err != nil || fetched {
			return err
		}
		if zones == nil {
			continue
		}
		entry, listed := zones[s.iata]
		endpoint, valid := "", false
		if listed {
			endpoint, valid = i.scopesURL(entry.url)
		}
		if !valid {
			// No request to make; names already imported stay until a catalogue replaces them.
			s.cache.NextAttempt, s.cache.LastError = now.Add(i.interval), "not listed"
			if listed {
				s.cache.LastError = "invalid site URL"
			}
			i.log(*s, "skipped")
			continue
		}
		if endpoint != s.url {
			s.url, s.cache.ETag = endpoint, ""
		}
		return i.refresh(ctx, s, now)
	}
	return nil
}

func (i *Importer) refresh(ctx context.Context, s *source, now time.Time) error {
	update := Cache{AttemptedAt: now, NextAttempt: now.Add(i.interval)}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.url, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "Beacon-MeshMapper-Scopes/1")
	if len(s.cache.Payload) > 0 && s.cache.ETag != "" {
		request.Header.Set("If-None-Match", s.cache.ETag)
	}
	// Recorded first: an abandoned request may still have used the region's call.
	attempt := Cache{AttemptedAt: now, NextAttempt: now.Add(min(i.interval, failureRetry)), LastError: "no response"}
	s.cache.NextAttempt = attempt.NextAttempt
	if err := i.store.SaveScopeCatalogue(ctx, s.iata, s.url, attempt, nil); err != nil {
		return fmt.Errorf("persist scope catalogue %s: %w", s.iata, err)
	}
	s.cache.AttemptedAt, s.cache.LastError = now, attempt.LastError
	response, err := i.client.Do(request)
	var entries []scopestore.Entry
	var generated time.Time
	var retryAfter time.Time
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		update.LastError = "request failed" // do not persist untrusted response bodies or URLs from errors
	} else {
		defer response.Body.Close()
		switch response.StatusCode {
		case http.StatusOK:
			body, readErr := io.ReadAll(io.LimitReader(response.Body, MaxBody+1))
			if readErr == nil {
				entries, generated, readErr = decode(body, s.iata)
			}
			if readErr != nil {
				update.LastError = "invalid response"
			} else {
				update.Payload = body
			}
		case http.StatusNotModified:
			if len(s.cache.Payload) == 0 || s.cache.ETag == "" {
				update.LastError = "304 without cached catalogue"
			}
		default:
			update.LastError = fmt.Sprintf("HTTP %d", response.StatusCode)
			if response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusServiceUnavailable {
				// The published contract uses seconds. Ignore invalid/overflowing headers.
				if seconds, parseErr := strconv.ParseInt(response.Header.Get("Retry-After"), 10, 64); parseErr == nil && seconds > 0 && seconds <= int64((1<<63-1)/time.Second) {
					retryAfter = now.Add(time.Duration(seconds) * time.Second)
				}
			}
		}
		if update.LastError == "" {
			etag := response.Header.Get("ETag")
			if len(etag) > 256 || strings.ContainsAny(etag, "\r\n") {
				update.LastError = "invalid ETag"
				update.Payload = nil
				entries = nil
			} else {
				update.ETag = etag
				if response.StatusCode == http.StatusNotModified && etag == "" {
					update.ETag = s.cache.ETag
				}
				update.CheckedAt = now
			}
		}
	}
	if update.LastError != "" {
		// Failures retry before the full interval, but never sooner than the rate limit or a Retry-After.
		update.NextAttempt = now.Add(min(i.interval, failureRetry))
		if retryAfter.After(update.NextAttempt) {
			update.NextAttempt = retryAfter
		}
	}
	if err := i.store.SaveScopeCatalogue(ctx, s.iata, s.url, update, entries); err != nil {
		// Avoid a retry every scheduler tick during a DB outage; never publish uncommitted keys.
		s.cache.NextAttempt = update.NextAttempt
		return fmt.Errorf("persist scope catalogue %s: %w", s.iata, err)
	}
	if len(update.Payload) > 0 {
		s.cache.Payload = update.Payload
		s.entries = entries
		s.generated = generated
	}
	if !update.CheckedAt.IsZero() {
		s.cache.CheckedAt = update.CheckedAt
		s.cache.ETag = update.ETag
	}
	s.cache.AttemptedAt = now
	s.cache.NextAttempt = update.NextAttempt
	s.cache.LastError = update.LastError
	if len(update.Payload) > 0 {
		i.publish()
		if i.onChange != nil {
			i.onChange(ctx)
		}
	}
	i.log(*s, "checked")
	return nil
}

func (i *Importer) publish() {
	byName := make(map[string]scopestore.Entry, len(i.manual))
	for _, e := range i.manual {
		byName[e.Name] = e
	}
	members := make(map[string][]string, len(i.sources))
	for _, s := range i.sources {
		for _, candidate := range s.entries {
			members[s.iata] = append(members[s.iata], candidate.Name)
			entry, exists := byName[candidate.Name]
			if exists && entry.IATAs == nil {
				continue
			} // manual metadata/keys win
			if !exists {
				entry = candidate
			}
			entry.IATAs = append(entry.IATAs, s.iata)
			byName[entry.Name] = entry
		}
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	slices.Sort(names)
	entries := make([]scopestore.Entry, 0, len(names))
	for _, name := range names {
		entries = append(entries, byName[name])
	}
	i.scopes.Load(entries)
	i.scopes.SetCatalogueMembers(members)
}

func (i *Importer) log(s source, action string) {
	level := slog.LevelInfo
	if s.cache.LastError != "" && s.cache.LastError != "not listed" {
		level = slog.LevelWarn
	}
	slog.Log(context.Background(), level, "MeshMapper scopes "+action, "component", "meshmapper.scopes", "iata", s.iata, "source", s.url,
		"names", len(s.entries), "generated_at", s.generated, "checked_at", s.cache.CheckedAt, "next_attempt", s.cache.NextAttempt, "last_error", s.cache.LastError)
}

func scopesEndpoint(site string) (string, bool) { return siteEndpoint(site, "get_scopes.php") }

func decode(body []byte, iata string) ([]scopestore.Entry, time.Time, error) {
	var document struct {
		GeneratedAt       time.Time `json:"generated_at"`
		Region            string    `json:"region"`
		Zones             []string  `json:"zones"`
		Repeaters, Scoped *int
		Scopes            *[]struct {
			Name                  string
			Repeaters, Default    *int
			Monitored, Wardriving *bool
		}
	}
	if len(body) > MaxBody || !utf8.Valid(body) {
		return nil, time.Time{}, fmt.Errorf("invalid catalogue size/encoding")
	}
	if err := json.Unmarshal(body, &document); err != nil {
		return nil, time.Time{}, err
	}
	if document.GeneratedAt.IsZero() || document.Region != iata || len(document.Zones) != 1 || document.Zones[0] != iata ||
		document.Repeaters == nil || document.Scoped == nil || *document.Scoped < 0 || *document.Repeaters < *document.Scoped ||
		document.Scopes == nil || len(*document.Scopes) > MaxScopes {
		return nil, time.Time{}, fmt.Errorf("invalid regional catalogue")
	}
	entries := make([]scopestore.Entry, 0, len(*document.Scopes))
	seen := map[string]bool{}
	for _, s := range *document.Scopes {
		if s.Name == "" || len(s.Name) > 128 || strings.TrimSpace(s.Name) != s.Name || strings.ContainsFunc(s.Name, unicode.IsControl) ||
			s.Repeaters == nil || s.Default == nil || s.Monitored == nil || s.Wardriving == nil ||
			*s.Default < 0 || *s.Repeaters < *s.Default || *s.Repeaters > *document.Scoped {
			return nil, time.Time{}, fmt.Errorf("invalid scope entry")
		}
		entry := scopestore.FromName(s.Name)
		if len(entry.Name) < 2 || seen[entry.Name] {
			return nil, time.Time{}, fmt.Errorf("empty or duplicate scope name")
		}
		seen[entry.Name] = true
		entries = append(entries, entry)
	}
	return entries, document.GeneratedAt, nil
}
