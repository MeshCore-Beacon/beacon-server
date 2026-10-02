// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshmapper

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
)

// RegionState is a stored region; imported ones are also what SaveImportedRegion writes.
type RegionState struct {
	Slug, Name           string
	DisplayOrder         int
	Imported             bool
	IATAs                []string
	CenterLat, CenterLng *float64
}

// IATADetails is an IATA's display name and approximate location.
type IATADetails struct {
	IATA     string
	Name     string
	Lat, Lng *float64
}

// SetConfiguredIATAs names the IATAs whose config entry sets a name or location; those win.
func (z *Zones) SetConfiguredIATAs(iatas []string) {
	z.configured = map[string]bool{}
	for _, iata := range iatas {
		z.configured[iata] = true
	}
}

// OnIATAsChange is told whenever imported IATA names or locations change.
func (z *Zones) OnIATAsChange(fn func(ctx context.Context)) { z.onIATAs = fn }

// OnRegionsChange is told whenever the imported regions change, for cache invalidation.
func (z *Zones) OnRegionsChange(fn func(ctx context.Context)) { z.onRegions = fn }

// pruneGroups drops every imported region when group import is off.
func (z *Zones) pruneGroups(ctx context.Context) error {
	pruned, err := z.store.PruneImportedRegions(ctx, nil)
	if err != nil {
		return fmt.Errorf("prune MeshMapper regions: %w", err)
	}
	z.regionsRemoved(pruned)
	if len(pruned) > 0 {
		z.regionsChanged(ctx)
	}
	return nil
}

// syncInputs is what syncDirectory last applied; it reruns when any of them changes.
type syncInputs struct {
	version, known int
	complete       bool
	heard          string
}

// syncDirectory applies the loaded zone lists to IATA details and, when enabled,
// imported regions. Groups are only pruned once every tracked country has a list.
func (z *Zones) syncDirectory(ctx context.Context, iatas []string) error {
	var countries []string
	for _, r := range z.regions {
		countries = append(countries, r.country)
	}
	slices.Sort(countries)
	groups, zones, version, complete := z.dir.snapshot(slices.Compact(countries))
	var heard []string
	if z.importGroups {
		var err error
		if heard, err = z.store.ListHeardIATAs(ctx); err != nil {
			return fmt.Errorf("list heard IATAs for MeshMapper groups: %w", err)
		}
		slices.Sort(heard)
	}
	inputs := syncInputs{version: version, known: len(iatas), complete: complete, heard: strings.Join(heard, ",")}
	if z.groupsSynced && inputs == z.synced {
		return nil
	}
	if err := z.syncIATADetails(ctx, zones); err != nil {
		return err
	}
	if z.importGroups {
		if err := z.syncGroups(ctx, heard, groups, zones, complete); err != nil {
			return err
		}
	}
	z.groupsSynced, z.synced = true, inputs
	return nil
}

// syncIATADetails names and locates each listed IATA unless its config entry does.
func (z *Zones) syncIATADetails(ctx context.Context, zones map[string]zoneEntry) error {
	stored, err := z.store.ListIATADetails(ctx)
	if err != nil {
		return fmt.Errorf("list IATAs for MeshMapper details: %w", err)
	}
	changed := false
	for _, cur := range stored {
		zone, ok := zones[cur.IATA]
		if !ok || z.configured[cur.IATA] || zone.name == "" && zone.lat == nil {
			continue
		}
		name := cmp.Or(zone.name, cur.Name)
		lat, lng := cur.Lat, cur.Lng
		if zone.lat != nil {
			lat, lng = zone.lat, zone.lon
		}
		if name == cur.Name && sameFloat(lat, cur.Lat) && sameFloat(lng, cur.Lng) {
			continue
		}
		if err := z.store.UpsertIATADetails(ctx, cur.IATA, name, lat, lng); err != nil {
			return fmt.Errorf("save MeshMapper details for %s: %w", cur.IATA, err)
		}
		changed = true
	}
	if changed && z.onIATAs != nil {
		z.onIATAs(ctx)
	}
	return nil
}

// syncGroups imports groups with a member that is heard or configured, never one only
// its own import created. Without every country's list it adds and updates but never prunes.
func (z *Zones) syncGroups(ctx context.Context, heard []string, groups map[string]zoneGroup, zones map[string]zoneEntry, complete bool) error {
	inUse := maps.Clone(z.configured)
	if inUse == nil {
		inUse = map[string]bool{}
	}
	for _, iata := range heard {
		inUse[iata] = true
	}
	state, err := z.store.ListRegionState(ctx)
	if err != nil {
		return fmt.Errorf("list regions for MeshMapper groups: %w", err)
	}
	stored, handOrder := map[string]RegionState{}, 0
	for _, r := range state {
		stored[r.Slug] = r
		if !r.Imported {
			handOrder = max(handOrder, r.DisplayOrder)
			for _, iata := range r.IATAs {
				inUse[iata] = true
			}
		}
	}
	var want []RegionState
	for code, g := range groups {
		if slices.ContainsFunc(g.members, func(m string) bool { return inUse[m] }) {
			lat, lng := center(g.members, zones)
			want = append(want, RegionState{Slug: strings.ToLower(code), Name: g.name, Imported: true, IATAs: g.members, CenterLat: lat, CenterLng: lng})
		}
	}
	slices.SortFunc(want, func(a, b RegionState) int { return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.Slug, b.Slug)) })
	keep, changed := []string{}, false
	for n, r := range want {
		r.DisplayOrder = handOrder + 1 + n
		if cur, ok := stored[r.Slug]; ok && !cur.Imported {
			slog.Info("MeshMapper group skipped: a configured region uses its slug", "component", "meshmapper.zones", "slug", r.Slug)
			continue
		} else if ok && cur.Name == r.Name && cur.DisplayOrder == r.DisplayOrder && slices.Equal(cur.IATAs, r.IATAs) &&
			sameFloat(cur.CenterLat, r.CenterLat) && sameFloat(cur.CenterLng, r.CenterLng) {
			keep = append(keep, r.Slug)
			continue
		}
		saved, err := z.store.SaveImportedRegion(ctx, r)
		if err != nil {
			return fmt.Errorf("save MeshMapper region %s: %w", r.Slug, err)
		}
		if !saved {
			slog.Info("MeshMapper group skipped: a configured region uses its slug", "component", "meshmapper.zones", "slug", r.Slug)
			continue
		}
		keep, changed = append(keep, r.Slug), true
		slog.Info("MeshMapper region imported", "component", "meshmapper.zones", "slug", r.Slug, "name", r.Name, "iatas", r.IATAs)
	}
	var pruned []string
	if complete {
		if pruned, err = z.store.PruneImportedRegions(ctx, keep); err != nil {
			return fmt.Errorf("prune MeshMapper regions: %w", err)
		}
		z.regionsRemoved(pruned)
	}
	if changed || len(pruned) > 0 {
		z.regionsChanged(ctx)
	}
	return nil
}

// center averages the members' zone locations; nil when none has one.
func center(members []string, zones map[string]zoneEntry) (*float64, *float64) {
	var lat, lng float64
	n := 0
	for _, m := range members {
		if z := zones[m]; z.lat != nil {
			lat, lng, n = lat+*z.lat, lng+*z.lon, n+1
		}
	}
	if n == 0 {
		return nil, nil
	}
	lat, lng = lat/float64(n), lng/float64(n)
	return &lat, &lng
}

func sameFloat(a, b *float64) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

func (z *Zones) regionsRemoved(slugs []string) {
	for _, slug := range slugs {
		slog.Info("MeshMapper region removed", "component", "meshmapper.zones", "slug", slug)
	}
}

func (z *Zones) regionsChanged(ctx context.Context) {
	if z.onRegions != nil {
		z.onRegions(ctx)
	}
}
