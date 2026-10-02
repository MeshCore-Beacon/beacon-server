// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshmapper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/iatadb"
)

const (
	ZonesURL        = "https://meshmapper.net/get_zones.php"
	MaxZoneList     = 1 << 20
	MaxBoundaryBody = 8 << 20 // outlines are never simplified

	// MeshMapper asks clients to allow 60-120s; a request given up on may still use the day's call.
	requestTimeout = 60 * time.Second
	refreshTimeout = requestTimeout + 30*time.Second // room for the DB writes around a slow request

	zoneListFresh    = config.MinZonesRefresh // get_zones.php allows one request per country per 23.5h
	zoneFailureRetry = config.MinZonesRefresh // failed requests count too
)

var zoneSite = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*\.meshmapper\.net$`)

// Boundary is one IATA's imported MeshMapper outline and its fetch state.
type Boundary struct {
	IATA, URL                           string
	Feature                             json.RawMessage
	ETag                                string
	CheckedAt, AttemptedAt, NextAttempt time.Time
	LastError                           string
}

type ZoneStore interface {
	ListKnownIATAs(ctx context.Context) ([]string, error)
	// ListHeardIATAs returns IATAs observers report from; group imports create IATAs too.
	ListHeardIATAs(ctx context.Context) ([]string, error)
	PruneZoneBoundaries(ctx context.Context, keep []string) ([]string, error)
	ListZoneBoundaries(ctx context.Context) ([]Boundary, error)
	SaveZoneBoundary(ctx context.Context, b Boundary) error
	ListRegionState(ctx context.Context) ([]RegionState, error)
	ListIATADetails(ctx context.Context) ([]IATADetails, error)
	UpsertIATADetails(ctx context.Context, iata, name string, lat, lng *float64) error
	SaveImportedRegion(ctx context.Context, r RegionState) (bool, error)
	PruneImportedRegions(ctx context.Context, keep []string) ([]string, error)
}

type zoneRegion struct {
	country string
	b       Boundary
}

// Zones is owned by one background task; listeners are wired before Restore.
type Zones struct {
	store     ZoneStore
	enabled   bool
	seen      map[string]bool
	interval  time.Duration
	client    *http.Client
	dir       *Directory
	boundsURL func(site string) (string, bool)
	regions   []*zoneRegion
	onChange  func(ctx context.Context, iata string)
	onUpdate  func(imported map[string]json.RawMessage)

	importGroups bool
	groupsSynced bool
	synced       syncInputs
	onRegions    func(ctx context.Context)
	configured   map[string]bool
	onIATAs      func(ctx context.Context)
}

func NewZones(cfg config.MeshMapperZonesConfig, store ZoneStore, dir *Directory) *Zones {
	return &Zones{store: store, enabled: cfg.Enabled, importGroups: cfg.Enabled && cfg.ImportGroups, seen: map[string]bool{}, interval: cfg.Interval(), dir: dir,
		boundsURL: boundaryEndpoint, client: &http.Client{
			Timeout:       requestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}}
}

// OnChange receives each IATA whose served border changed, for cache invalidation.
func (z *Zones) OnChange(fn func(ctx context.Context, iata string)) { z.onChange = fn }

// OnUpdate receives every imported boundary whenever the set changes.
func (z *Zones) OnUpdate(fn func(imported map[string]json.RawMessage)) { z.onUpdate = fn }

// Restore prunes imports for unknown IATAs (all of them when disabled), and
// imported regions when group import is off, then loads saved boundaries,
// without making HTTP requests.
func (z *Zones) Restore(ctx context.Context) error {
	if !z.importGroups {
		if err := z.pruneGroups(ctx); err != nil {
			return err
		}
	}
	iatas, err := z.store.ListKnownIATAs(ctx)
	if err != nil {
		return fmt.Errorf("list IATAs for MeshMapper boundaries: %w", err)
	}
	keep := []string{}
	if z.enabled {
		keep = iatas
	}
	pruned, err := z.store.PruneZoneBoundaries(ctx, keep)
	if err != nil {
		return fmt.Errorf("prune MeshMapper boundaries: %w", err)
	}
	for _, iata := range pruned {
		slog.Info("MeshMapper boundary removed", "component", "meshmapper.zones", "iata", iata)
		z.changed(ctx, iata)
	}
	if !z.enabled {
		return nil
	}
	saved, err := z.store.ListZoneBoundaries(ctx)
	if err != nil {
		return fmt.Errorf("restore MeshMapper boundaries: %w", err)
	}
	byIATA := map[string]Boundary{}
	for _, b := range saved {
		byIATA[b.IATA] = b
	}
	z.track(iatas, byIATA)
	z.publish()
	return nil
}

// track adds a region for each IATA not seen before.
func (z *Zones) track(iatas []string, saved map[string]Boundary) {
	for _, iata := range iatas {
		if z.seen[iata] {
			continue
		}
		z.seen[iata] = true
		country := iatadb.CountryFor(iata)
		if country == "" {
			slog.Warn("MeshMapper boundary skipped: IATA has no known country", "component", "meshmapper.zones", "iata", iata)
			continue
		}
		b, ok := saved[iata]
		if !ok {
			b = Boundary{IATA: iata}
		}
		z.regions = append(z.regions, &zoneRegion{country: country, b: b})
		if ok {
			z.log(b, "restored")
		}
	}
}

// Refresh makes at most one request. The scheduler serializes calls every 15s.
func (z *Zones) Refresh(ctx context.Context) (err error) {
	parent := ctx
	defer func() {
		if parent.Err() != nil {
			err = nil
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()
	now := time.Now().UTC()
	// IATAs are created from traffic, so pick up new ones every tick.
	iatas, err := z.store.ListKnownIATAs(ctx)
	if err != nil {
		return fmt.Errorf("list IATAs for MeshMapper boundaries: %w", err)
	}
	z.track(iatas, nil)
	if err := z.syncDirectory(ctx, iatas); err != nil {
		return err
	}
	for _, r := range z.regions {
		if now.Before(r.b.NextAttempt) {
			continue
		}
		zones, fetched, err := z.dir.List(ctx, r.country, now)
		if err != nil || fetched {
			return err
		}
		if zones == nil {
			continue
		}
		return z.refresh(ctx, r, zones, now)
	}
	return nil
}

func (z *Zones) refresh(ctx context.Context, r *zoneRegion, zones map[string]zoneEntry, now time.Time) error {
	update := Boundary{IATA: r.b.IATA, URL: r.b.URL, AttemptedAt: now, NextAttempt: now.Add(z.interval)}
	var retryAt time.Time
	entry, listed := zones[r.b.IATA]
	endpoint, valid := "", false
	if listed {
		update.URL = entry.url
		endpoint, valid = z.boundsURL(entry.url)
	}
	switch {
	case !listed:
		update.LastError = "not listed"
	case !entry.hasBoundary:
		update.LastError = "no boundary"
	case !valid:
		update.LastError = "invalid site URL"
	default:
		etag := ""
		if r.b.Feature != nil {
			etag = r.b.ETag
		}
		// Recorded first: an abandoned request may still have used the region's call.
		attempt := Boundary{IATA: r.b.IATA, URL: update.URL, AttemptedAt: now, NextAttempt: now.Add(min(z.interval, zoneFailureRetry)), LastError: "no response"}
		r.b.NextAttempt = attempt.NextAttempt
		if err := z.store.SaveZoneBoundary(ctx, attempt); err != nil {
			return fmt.Errorf("persist MeshMapper boundary %s: %w", r.b.IATA, err)
		}
		r.b.URL, r.b.AttemptedAt, r.b.LastError = attempt.URL, now, attempt.LastError
		status, body, header, err := get(ctx, z.client, "Beacon-MeshMapper-Zones/1", endpoint, etag, MaxBoundaryBody)
		if err != nil {
			return err
		}
		switch status {
		case http.StatusOK:
			feature, decodeErr := decodeBoundary(body, r.b.IATA)
			switch {
			case decodeErr != nil:
				update.LastError = "invalid response" // includes truncated bodies; retry
			case feature == nil:
				update.LastError = "no boundary"
			default:
				update.Feature = feature
			}
		case http.StatusNotModified:
			if r.b.Feature == nil {
				update.LastError = "304 without cached boundary"
			}
		default:
			update.LastError = statusProblem(status, header, now, &retryAt)
		}
		if update.LastError == "" {
			etag := header.Get("ETag")
			if len(etag) > 256 || strings.ContainsAny(etag, "\r\n") {
				update.LastError, update.Feature = "invalid ETag", nil
			} else {
				if status == http.StatusNotModified && etag == "" {
					etag = r.b.ETag
				}
				update.ETag, update.CheckedAt = etag, now
			}
		}
		if update.LastError != "" && update.LastError != "no boundary" {
			update.NextAttempt = now.Add(min(z.interval, zoneFailureRetry))
			if retryAt.After(update.NextAttempt) {
				update.NextAttempt = retryAt
			}
		}
	}
	if err := z.store.SaveZoneBoundary(ctx, update); err != nil {
		r.b.NextAttempt = update.NextAttempt // avoid retrying every tick during a DB outage
		return fmt.Errorf("persist MeshMapper boundary %s: %w", r.b.IATA, err)
	}
	changed := update.Feature != nil && !bytes.Equal(update.Feature, r.b.Feature)
	if update.Feature != nil {
		r.b.Feature = update.Feature
	}
	if !update.CheckedAt.IsZero() {
		r.b.CheckedAt, r.b.ETag = update.CheckedAt, update.ETag
	}
	r.b.URL, r.b.AttemptedAt, r.b.NextAttempt, r.b.LastError = update.URL, now, update.NextAttempt, update.LastError
	if changed {
		z.changed(ctx, r.b.IATA)
		z.publish()
	}
	z.log(r.b, "checked")
	return nil
}

// get returns transport errors only when ctx ended; other failures become status 0.
func get(ctx context.Context, client *http.Client, agent, endpoint, etag string, limit int64) (int, []byte, http.Header, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", agent)
	if etag != "" {
		request.Header.Set("If-None-Match", etag)
	}
	response, err := client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return 0, nil, nil, ctx.Err()
		}
		return 0, nil, http.Header{}, nil
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return response.StatusCode, nil, response.Header, nil
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil || int64(len(body)) > limit {
		return response.StatusCode, nil, response.Header, nil
	}
	return response.StatusCode, body, response.Header, nil
}

// statusProblem describes a failed response and reads Retry-After on 429/503.
func statusProblem(status int, header http.Header, now time.Time, retryAt *time.Time) string {
	if status == 0 {
		return "request failed" // don't persist untrusted error text
	}
	if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable {
		if seconds, err := strconv.ParseInt(header.Get("Retry-After"), 10, 64); err == nil && seconds > 0 && seconds <= int64((1<<63-1)/time.Second) {
			*retryAt = now.Add(time.Duration(seconds) * time.Second)
		}
	}
	return fmt.Sprintf("HTTP %d", status)
}

func (z *Zones) changed(ctx context.Context, iata string) {
	if z.onChange != nil {
		z.onChange(ctx, iata)
	}
}

func (z *Zones) publish() {
	if z.onUpdate == nil {
		return
	}
	imported := map[string]json.RawMessage{}
	for _, r := range z.regions {
		if r.b.Feature != nil {
			imported[r.b.IATA] = r.b.Feature
		}
	}
	z.onUpdate(imported)
}

func (z *Zones) log(b Boundary, action string) {
	level := slog.LevelInfo
	if b.LastError != "" && b.LastError != "no boundary" && b.LastError != "not listed" {
		level = slog.LevelWarn
	}
	slog.Log(context.Background(), level, "MeshMapper boundary "+action, "component", "meshmapper.zones", "iata", b.IATA,
		"source", b.URL, "imported", b.Feature != nil, "checked_at", b.CheckedAt, "next_attempt", b.NextAttempt, "last_error", b.LastError)
}

func boundaryEndpoint(site string) (string, bool) { return siteEndpoint(site, "get_geojson.php") }

// siteEndpoint accepts only a published region site root.
func siteEndpoint(site, file string) (string, bool) {
	u, err := url.Parse(site)
	if err != nil || u.Scheme != "https" || u.User != nil || !zoneSite.MatchString(u.Host) ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return "", false
	}
	return "https://" + u.Host + "/" + file, true
}

// decodeBoundary returns nil when MeshMapper has no usable outline for the region.
func decodeBoundary(body []byte, iata string) (json.RawMessage, error) {
	var collection struct {
		Type     string            `json:"type"`
		Features []json.RawMessage `json:"features"`
	}
	if err := json.Unmarshal(body, &collection); err != nil {
		return nil, err
	}
	if collection.Type != "FeatureCollection" || len(collection.Features) != 1 {
		return nil, fmt.Errorf("expected one region feature")
	}
	var feature struct {
		Geometry   json.RawMessage `json:"geometry"`
		Properties struct {
			Code string `json:"code"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(collection.Features[0], &feature); err != nil {
		return nil, err
	}
	if !strings.EqualFold(feature.Properties.Code, iata) {
		return nil, fmt.Errorf("feature is for another region")
	}
	if len(feature.Geometry) == 0 || string(feature.Geometry) == "null" {
		return nil, nil
	}
	validated, err := config.ValidateBorder(collection.Features[0])
	if err != nil {
		return nil, err
	}
	// Foreign marking must accept anything the map serves.
	if _, err := config.BuildLocalBorders(nil, map[string]json.RawMessage{iata: validated}); err != nil {
		return nil, err
	}
	return validated, nil
}
