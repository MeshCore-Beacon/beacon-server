// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshmapper

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

const groupedList = `{"country":"CA","zones":[` +
	`{"code":"YOW","name":"Ottawa, CA","short_name":"Ottawa","lat":45,"lon":-76,"url":"URL/","has_boundary":true,"group":"ONQC"},` +
	`{"code":"YUL","name":"Montreal, CA","lat":46,"lon":-74,"url":"URL/","has_boundary":true,"group":"ONQC"},` +
	`{"code":"YQB","name":"Quebec City, CA","url":"URL/","has_boundary":true,"group":"ONQC"}],"groups":[` +
	`{"code":"ONQC","name":"Ottawa-Quebec Corridor","members":["YUL","YOW","YQB"],"url":"https://onqc.meshmapper.net/"},` +
	`{"code":"GOLM","name":"Great Ontario Lake Mesh","members":["YYZ","YKF"],"url":"https://golm.meshmapper.net/"}]}`

type groupHarness struct {
	z           *Zones
	f           *fakeMeshMapper
	store       *zoneMemoryStore
	invalidated int
}

func newGroupHarness(t *testing.T, f *fakeMeshMapper, store *zoneMemoryStore, importGroups bool) *groupHarness {
	t.Helper()
	if store.iatas == nil {
		store.iatas = []string{"YOW"}
	}
	if store.heard == nil {
		store.heard = slices.Clone(store.iatas)
	}
	if store.rows == nil {
		store.rows = map[string]Boundary{}
	}
	h := &groupHarness{f: f, store: store}
	dir := NewDirectory(newZoneListMemory())
	dir.listURL = f.URL + "/get_zones.php"
	h.z = NewZones(config.MeshMapperZonesConfig{Enabled: true, ImportGroups: importGroups}, store, dir)
	h.z.boundsURL = func(site string) (string, bool) { return site + "get_geojson.php", true }
	h.z.OnRegionsChange(func(context.Context) { h.invalidated++ })
	if err := h.z.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	return h
}

func groupedFake(t *testing.T) *fakeMeshMapper {
	f := newFakeMeshMapper(t)
	f.list = strings.ReplaceAll(groupedList, "URL", f.URL)
	return f
}

// refetch makes the country list and YOW due, then runs the fetch and the sync tick.
func (h *groupHarness) refetch(t *testing.T) {
	t.Helper()
	if list := h.z.dir.lists["CA"]; list != nil {
		list.fetchedAt, list.nextAttempt = time.Time{}, time.Time{}
	}
	for _, r := range h.z.regions {
		r.b.NextAttempt = time.Time{}
	}
	for range 2 {
		if err := h.z.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGroupImportedWhenAMemberIsKnown(t *testing.T) {
	h := newGroupHarness(t, groupedFake(t), &zoneMemoryStore{}, true)
	h.refetch(t)
	got, ok := h.store.regions["onqc"]
	if !ok || got.Name != "Ottawa-Quebec Corridor" || !slices.Equal(got.IATAs, []string{"YOW", "YQB", "YUL"}) || got.DisplayOrder != 1 {
		t.Fatalf("group not imported with full members: %+v", h.store.regions)
	}
	if _, ok := h.store.regions["golm"]; ok {
		t.Fatal("group with no known member imported")
	}
	if got.CenterLat == nil || *got.CenterLat != 45.5 || *got.CenterLng != -75 {
		t.Fatalf("center should average the located members: %v, %v", got.CenterLat, got.CenterLng)
	}
	if !slices.Contains(h.store.iatas, "YQB") || !slices.Contains(h.store.iatas, "YUL") {
		t.Fatal("members not created as IATAs", h.store.iatas)
	}
	if h.invalidated != 1 {
		t.Fatal("region caches not invalidated", h.invalidated)
	}
	h.refetch(t)
	if h.invalidated != 1 {
		t.Fatal("unchanged groups invalidated again", h.invalidated)
	}
}

func TestGroupSlugClashKeepsConfiguredRegion(t *testing.T) {
	hand := RegionState{Slug: "onqc", Name: "Mine", DisplayOrder: 4, IATAs: []string{"YOW"}}
	store := &zoneMemoryStore{regions: map[string]RegionState{"onqc": hand}}
	h := newGroupHarness(t, groupedFake(t), store, true)
	h.refetch(t)
	if got := store.regions["onqc"]; got.Imported || got.Name != "Mine" || !slices.Equal(got.IATAs, []string{"YOW"}) || h.invalidated != 0 {
		t.Fatalf("configured region replaced: %+v", got)
	}
}

func TestGroupDroppedRemovesRegion(t *testing.T) {
	h := newGroupHarness(t, groupedFake(t), &zoneMemoryStore{}, true)
	h.refetch(t)
	h.f.list = strings.ReplaceAll(`{"country":"CA","zones":[{"code":"YOW","url":"URL/","has_boundary":true,"group":null}],"groups":[]}`, "URL", h.f.URL)
	h.refetch(t)
	if len(h.store.regions) != 0 || h.invalidated != 2 {
		t.Fatalf("dropped group kept: %+v invalidated=%d", h.store.regions, h.invalidated)
	}
}

func TestGroupImportOffRemovesImportedRegions(t *testing.T) {
	hand := RegionState{Slug: "east", Name: "East", IATAs: []string{"YOW"}}
	imported := RegionState{Slug: "onqc", Name: "Ottawa-Quebec Corridor", Imported: true, IATAs: []string{"YOW"}}
	store := &zoneMemoryStore{regions: map[string]RegionState{"east": hand, "onqc": imported}}
	h := newGroupHarness(t, groupedFake(t), store, false)
	if _, ok := store.regions["onqc"]; ok || h.invalidated != 1 {
		t.Fatal("imported region kept with import_groups off")
	}
	if _, ok := store.regions["east"]; !ok {
		t.Fatal("configured region removed")
	}
	h.refetch(t)
	if len(store.regions) != 1 {
		t.Fatal("groups imported with import_groups off", store.regions)
	}
}

func TestGroupFailedFetchKeepsGroups(t *testing.T) {
	h := newGroupHarness(t, groupedFake(t), &zoneMemoryStore{}, true)
	h.refetch(t)
	h.f.listStatus = 500
	h.z.groupsSynced = false // force a reconcile against the retained list
	h.refetch(t)
	if _, ok := h.store.regions["onqc"]; !ok {
		t.Fatal("failed fetch dropped imported region")
	}
}

func TestGroupColdStartWaitsForEveryList(t *testing.T) {
	old := RegionState{Slug: "old", Name: "Old", Imported: true, IATAs: []string{"YOW"}}
	store := &zoneMemoryStore{regions: map[string]RegionState{"old": old}}
	f := groupedFake(t)
	f.listStatus = 500
	h := newGroupHarness(t, f, store, true)
	h.refetch(t)
	if _, ok := store.regions["old"]; !ok {
		t.Fatal("imported regions pruned before any list loaded")
	}
}

func TestZoneListNamesAndLocatesIATAs(t *testing.T) {
	h := newGroupHarness(t, groupedFake(t), &zoneMemoryStore{iatas: []string{"YOW", "YUL", "YQB", "YYZ"}}, false)
	h.z.SetConfiguredIATAs([]string{"YUL"})
	changed := 0
	h.z.OnIATAsChange(func(context.Context) { changed++ })
	h.refetch(t)
	yow := h.store.details["YOW"]
	if yow.Name != "Ottawa" || yow.Lat == nil || *yow.Lat != 45 || *yow.Lng != -76 {
		t.Fatalf("YOW not named and located from short_name: %+v", yow)
	}
	if yqb := h.store.details["YQB"]; yqb.Name != "Quebec City, CA" || yqb.Lat != nil {
		t.Fatalf("YQB should fall back to name and stay unlocated: %+v", yqb)
	}
	if _, ok := h.store.details["YUL"]; ok {
		t.Fatal("configured IATA overwritten")
	}
	if _, ok := h.store.details["YYZ"]; ok {
		t.Fatal("unlisted IATA written")
	}
	if changed != 1 || h.store.writes != 2 {
		t.Fatal("writes/invalidations", h.store.writes, changed)
	}
	h.z.groupsSynced = false
	h.refetch(t)
	if h.store.writes != 2 || changed != 1 {
		t.Fatal("unchanged details rewritten", h.store.writes, changed)
	}
}

func TestGroupStopsQualifyingOnImportedMembers(t *testing.T) {
	h := newGroupHarness(t, groupedFake(t), &zoneMemoryStore{}, true)
	h.refetch(t)
	if _, ok := h.store.regions["onqc"]; !ok {
		t.Fatal("group not imported")
	}
	without := strings.Replace(groupedList, `"members":["YUL","YOW","YQB"]`, `"members":["YUL","YQB"]`, 1)
	h.f.list = strings.ReplaceAll(without, "URL", h.f.URL)
	h.refetch(t)
	if _, ok := h.store.regions["onqc"]; ok {
		t.Fatal("group kept only by members its own import created")
	}
}

func TestGroupQualifiesThroughConfiguredRegion(t *testing.T) {
	hand := RegionState{Slug: "east", Name: "East", IATAs: []string{"YYZ"}}
	h := newGroupHarness(t, groupedFake(t), &zoneMemoryStore{regions: map[string]RegionState{"east": hand}}, true)
	h.refetch(t)
	if _, ok := h.store.regions["golm"]; !ok {
		t.Fatal("group with a configured member not imported")
	}
}

func TestGroupImportsFromListsItHas(t *testing.T) {
	old := RegionState{Slug: "old", Name: "Old", Imported: true, IATAs: []string{"YOW"}}
	store := &zoneMemoryStore{iatas: []string{"YOW", "SEA"}, regions: map[string]RegionState{"old": old}}
	f := groupedFake(t)
	f.otherStatus = 500
	h := newGroupHarness(t, f, store, true)
	h.refetch(t)
	for range 2 {
		if err := h.z.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if f.otherStatus != 500 || h.z.dir.lists["US"] == nil || h.z.dir.lists["US"].zones != nil {
		t.Fatal("US list should have failed")
	}
	if _, ok := store.regions["onqc"]; !ok {
		t.Fatal("one failed country blocked group import")
	}
	if store.details["YOW"].Name != "Ottawa" {
		t.Fatal("one failed country blocked IATA details", store.details)
	}
	if _, ok := store.regions["old"]; !ok {
		t.Fatal("pruned with a country list missing")
	}
}
