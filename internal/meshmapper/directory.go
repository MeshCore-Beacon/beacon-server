// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshmapper

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"
)

type zoneEntry struct {
	url         string
	hasBoundary bool
	name        string   // short display name, e.g. "Nanaimo"
	lat, lon    *float64 // nil when missing or out of range
}

type zoneGroup struct {
	name    string
	members []string
}

type zoneList struct {
	zones                  map[string]zoneEntry
	groups                 map[string]zoneGroup
	etag                   string
	fetchedAt, nextAttempt time.Time
}

// ZoneList is one country's saved get_zones.php response and fetch state.
type ZoneList struct {
	Country                             string
	Payload                             json.RawMessage
	ETag                                string
	FetchedAt, AttemptedAt, NextAttempt time.Time
	LastError                           string
}

type DirectoryStore interface {
	ListZoneLists(ctx context.Context) ([]ZoneList, error)
	SaveZoneList(ctx context.Context, l ZoneList) error
}

// Directory caches MeshMapper's per-country zone lists, shared by the zones and scopes imports.
type Directory struct {
	mu      sync.Mutex
	store   DirectoryStore
	client  *http.Client
	listURL string
	lists   map[string]*zoneList
	version int // bumped whenever a list is replaced
}

func NewDirectory(store DirectoryStore) *Directory {
	return &Directory{store: store, listURL: ZonesURL, lists: map[string]*zoneList{}, client: &http.Client{
		Timeout:       requestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

// Restore loads saved lists so a restart doesn't spend each country's daily request.
func (d *Directory) Restore(ctx context.Context) error {
	saved, err := d.store.ListZoneLists(ctx)
	if err != nil {
		return fmt.Errorf("restore MeshMapper zone lists: %w", err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, l := range saved {
		list := &zoneList{nextAttempt: l.NextAttempt}
		if zones, groups, err := decodeZones(l.Payload, l.Country); err == nil {
			list.zones, list.groups, list.etag, list.fetchedAt = zones, groups, l.ETag, l.FetchedAt
			d.version++
		}
		d.lists[l.Country] = list
	}
	return nil
}

// List returns the country's fresh zone list. fetched reports that this call
// spent the caller's one request; nil zones without fetched means back off.
func (d *Directory) List(ctx context.Context, country string, now time.Time) (zones map[string]zoneEntry, fetched bool, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	list := d.lists[country]
	if list != nil && list.zones != nil && now.Sub(list.fetchedAt) < zoneListFresh {
		return list.zones, false, nil
	}
	if list != nil && now.Before(list.nextAttempt) {
		return nil, false, nil
	}
	return nil, true, d.fetch(ctx, country, now)
}

func (d *Directory) fetch(ctx context.Context, country string, now time.Time) error {
	list := d.lists[country]
	if list == nil {
		list = &zoneList{}
		d.lists[country] = list
	}
	endpoint := d.listURL + "?country=" + url.QueryEscape(country)
	etag := ""
	if list.zones != nil {
		etag = list.etag
	}
	status, body, header, err := get(ctx, d.client, "Beacon-MeshMapper-Zones/1", endpoint, etag, MaxZoneList)
	if err != nil {
		return err
	}
	problem := ""
	var payload json.RawMessage
	var retryAt time.Time
	switch status {
	case http.StatusOK:
		zones, groups, decodeErr := decodeZones(body, country)
		if decodeErr != nil {
			problem = "invalid response"
		} else {
			list.zones, list.groups, list.etag = zones, groups, header.Get("ETag")
			payload = body
			d.version++
		}
	case http.StatusNotModified:
		if list.zones == nil {
			problem = "304 without cached list"
		}
	default:
		problem = statusProblem(status, header, now, &retryAt)
	}
	// Every request counts against the country's 23.5h allowance, failed or not.
	list.nextAttempt = now.Add(zoneListFresh)
	if retryAt.After(list.nextAttempt) {
		list.nextAttempt = retryAt
	}
	saved := ZoneList{Country: country, Payload: payload, AttemptedAt: now, NextAttempt: list.nextAttempt, LastError: problem}
	if problem == "" {
		list.fetchedAt = now
		saved.FetchedAt, saved.ETag = now, list.etag
	}
	level := slog.LevelInfo
	if problem != "" {
		level = slog.LevelWarn
	}
	slog.Log(ctx, level, "MeshMapper zone list checked", "component", "meshmapper.zones", "country", country,
		"zones", len(list.zones), "groups", len(list.groups), "next_attempt", list.nextAttempt, "last_error", problem)
	if err := d.store.SaveZoneList(ctx, saved); err != nil {
		return fmt.Errorf("persist MeshMapper zone list %s: %w", country, err)
	}
	return nil
}

// snapshot merges every loaded list's zones and groups by code. complete is false until
// each country has a list, so a cold start can't look like every group disappearing.
func (d *Directory) snapshot(countries []string) (merged map[string]zoneGroup, zones map[string]zoneEntry, version int, complete bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	merged, zones = map[string]zoneGroup{}, map[string]zoneEntry{}
	for _, country := range countries {
		list := d.lists[country]
		if list == nil || list.zones == nil {
			return nil, nil, d.version, false
		}
		for code, z := range list.zones {
			zones[code] = z
		}
		for code, g := range list.groups {
			if have, ok := merged[code]; ok {
				g.members = append(slices.Clone(have.members), g.members...)
				slices.Sort(g.members)
				g.members = slices.Compact(g.members)
				g.name = have.name
			}
			merged[code] = g
		}
	}
	return merged, zones, d.version, true
}

var (
	groupCode  = regexp.MustCompile(`^[A-Z0-9]{2,16}$`)
	memberIATA = regexp.MustCompile(`^[A-Z]{3}$`)
)

func decodeZones(body []byte, country string) (map[string]zoneEntry, map[string]zoneGroup, error) {
	var document struct {
		Country string `json:"country"`
		Zones   *[]struct {
			Code        string   `json:"code"`
			Name        string   `json:"name"`
			ShortName   string   `json:"short_name"`
			Lat         *float64 `json:"lat"`
			Lon         *float64 `json:"lon"`
			URL         string   `json:"url"`
			HasBoundary bool     `json:"has_boundary"`
		} `json:"zones"`
		Groups []struct {
			Code    string   `json:"code"`
			Name    string   `json:"name"`
			Members []string `json:"members"`
		} `json:"groups"`
	}
	if err := json.Unmarshal(body, &document); err != nil {
		return nil, nil, err
	}
	if !strings.EqualFold(document.Country, country) || document.Zones == nil {
		return nil, nil, fmt.Errorf("invalid zone list")
	}
	zones := make(map[string]zoneEntry, len(*document.Zones))
	for _, zone := range *document.Zones {
		entry := zoneEntry{url: zone.URL, hasBoundary: zone.HasBoundary, name: cmp.Or(zone.ShortName, zone.Name)}
		if entry.name = strings.TrimSpace(entry.name); len(entry.name) > 128 || strings.ContainsFunc(entry.name, unicode.IsControl) {
			entry.name = ""
		}
		if zone.Lat != nil && zone.Lon != nil && *zone.Lat >= -90 && *zone.Lat <= 90 && *zone.Lon >= -180 && *zone.Lon <= 180 {
			entry.lat, entry.lon = zone.Lat, zone.Lon
		}
		zones[strings.ToUpper(zone.Code)] = entry
	}
	groups := map[string]zoneGroup{}
	for _, g := range document.Groups {
		code, name := strings.ToUpper(g.Code), strings.TrimSpace(g.Name)
		if !groupCode.MatchString(code) || name == "" || len(name) > 128 || strings.ContainsFunc(name, unicode.IsControl) {
			continue // a bad group never blocks the zone list
		}
		var members []string
		for _, m := range g.Members {
			if m = strings.ToUpper(m); memberIATA.MatchString(m) {
				members = append(members, m)
			}
		}
		slices.Sort(members)
		if members = slices.Compact(members); len(members) > 0 {
			groups[code] = zoneGroup{name: name, members: members}
		}
	}
	return zones, groups, nil
}
