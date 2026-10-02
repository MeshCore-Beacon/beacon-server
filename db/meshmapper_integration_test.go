// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/meshmapper"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMeshMapperCataloguePostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	for _, table := range []string{"iata_codes", "transport_scopes", "meshmapper_scope_catalogues"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	store := &Store{q: sqlc.New(tx)}
	manual := scopestore.FromName("manual")
	manual.TransportKey[0] ^= 1
	if err := store.UpsertTransportScope(ctx, manual.Name, "Operator label", manual.TransportKey, manual.KeyFingerprint); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertIATA(ctx, "YOW"); err != nil {
		t.Fatal(err)
	}
	url := "https://yow.meshmapper.net/get_scopes.php"
	if row, err := store.GetScopeCatalogue(ctx, "YOW", url); err != nil || row != nil {
		t.Fatal(row, err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	payload := []byte(`{"generated_at":"2026-09-27T16:58:49Z","region":"YOW","zones":["YOW"],"repeaters":5,"scoped":3,"scopes":[{"name":"yow","repeaters":0,"default":0,"monitored":true,"wardriving":false}]}`)
	update := meshmapper.Cache{Payload: payload, ETag: `"v1"`, CheckedAt: now, AttemptedAt: now, NextAttempt: now.Add(time.Hour)}
	if err := store.SaveScopeCatalogue(ctx, "YOW", url, update, []scopestore.Entry{scopestore.FromName("manual"), scopestore.FromName("yow")}); err != nil {
		t.Fatal(err)
	}
	manualRows, err := store.GetTransportScopes(ctx)
	if err != nil || len(manualRows) != 1 || manualRows[0].TransportKey[0] != manual.TransportKey[0] {
		t.Fatal("manual key changed", manualRows, err)
	}
	var label string
	var imported bool
	if err := tx.QueryRow(ctx, "SELECT display_name,imported_only FROM transport_scopes WHERE name='#manual'").Scan(&label, &imported); err != nil || label != "Operator label" || imported {
		t.Fatal(label, imported, err)
	}
	row, err := store.GetScopeCatalogue(ctx, "YOW", url)
	if err != nil || row.ETag != `"v1"` || !row.CheckedAt.Equal(now) {
		t.Fatal(row, err)
	}
	scopes := scopestore.New()
	_, err = meshmapper.New(ctx, config.MeshMapperScopesConfig{Enabled: true}, store, meshmapper.NewDirectory(store), scopes, manualRows)
	if err != nil || len(scopes.Entries()) != 2 {
		t.Fatal("restart did not restore imported membership", err)
	}
	// A conditional success preserves the document, advances checked time and clears a failure.
	checked := now.Add(time.Hour)
	if err := store.SaveScopeCatalogue(ctx, "YOW", url, meshmapper.Cache{ETag: `"v1"`, CheckedAt: checked, AttemptedAt: checked, NextAttempt: checked.Add(time.Hour)}, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveScopeCatalogue(ctx, "YOW", url, meshmapper.Cache{AttemptedAt: checked.Add(time.Minute), NextAttempt: checked.Add(2 * time.Hour), LastError: "HTTP 429"}, nil); err != nil {
		t.Fatal(err)
	}
	row, err = store.GetScopeCatalogue(ctx, "YOW", url)
	if err != nil || !row.CheckedAt.Equal(checked) || row.ETag != `"v1"` || row.LastError != "HTTP 429" || len(row.Payload) == 0 {
		t.Fatal("failed refresh lost durable snapshot", row, err)
	}
	// Empty membership deactivates matching but retains the old scope identity for historical rows.
	empty := []byte(`{"generated_at":"2026-09-27T17:58:49Z","region":"YOW","zones":["YOW"],"repeaters":5,"scoped":0,"scopes":[]}`)
	update.Payload = empty
	update.ETag = ""
	if err := store.SaveScopeCatalogue(ctx, "YOW", url, update, nil); err != nil {
		t.Fatal(err)
	}
	_, err = meshmapper.New(ctx, config.MeshMapperScopesConfig{Enabled: true}, store, meshmapper.NewDirectory(store), scopes, manualRows)
	if err != nil || len(scopes.Entries()) != 1 {
		t.Fatal("removed imported membership remained active", err)
	}
	if _, err := store.GetTransportScopeByName(ctx, "#yow"); err != nil {
		t.Fatal("deleted historical identity", err)
	}
	row, _ = store.GetScopeCatalogue(ctx, "YOW", url)
	if row.ETag != "" {
		t.Fatal("200 without ETag retained obsolete validator")
	}
	// Reconfiguring a known imported name as manual promotes it without losing its identity.
	yow := scopestore.FromName("yow")
	if err := store.UpsertTransportScope(ctx, yow.Name, "Local", yow.TransportKey, yow.KeyFingerprint); err != nil {
		t.Fatal(err)
	}
	manualRows, err = store.GetTransportScopes(ctx)
	if err != nil || len(manualRows) != 2 {
		t.Fatal("manual promotion failed", err)
	}
	// A failed cache write also rolls back the identity insertion in the same statement.
	if _, err := tx.Exec(ctx, "ALTER TABLE meshmapper_scope_catalogues ADD CHECK(last_error <> 'force-failure'); SAVEPOINT rejected"); err != nil {
		t.Fatal(err)
	}
	update.LastError = "force-failure"
	if err := store.SaveScopeCatalogue(ctx, "YOW", url, update, []scopestore.Entry{scopestore.FromName("uncommitted")}); err == nil {
		t.Fatal("expected constraint failure")
	}
	if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT rejected"); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM transport_scopes WHERE name='#uncommitted'").Scan(&count); err != nil || count != 0 {
		t.Fatal("partially committed catalogue", count, err)
	}
}

func TestMeshMapperZoneBoundariesPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	for _, table := range []string{"iata_codes", "meshmapper_zone_boundaries", "meshmapper_zone_lists"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	store := &Store{q: sqlc.New(tx)}
	manual := []byte(`{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,1],[0,0]]]},"properties":{"source":"file"}}`)
	imported := []byte(`{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[2,0],[2,2],[0,2],[0,0]]]},"properties":{"code":"YOW"}}`)
	if err := store.UpsertIATABorder(ctx, "YOW", manual); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertIATA(ctx, "YYZ"); err != nil {
		t.Fatal(err)
	}
	border := func(iata string) string {
		t.Helper()
		got, err := store.GetIATABorder(ctx, iata)
		if err != nil {
			t.Fatal(err)
		}
		return string(got)
	}
	if !strings.Contains(border("YOW"), `"file"`) || border("YYZ") != "" {
		t.Fatal("manual border not served without an import")
	}
	now := time.Now().UTC().Truncate(time.Second)
	good := meshmapper.Boundary{IATA: "YOW", URL: "https://yow.meshmapper.net/", Feature: imported, ETag: `"v1"`, CheckedAt: now, AttemptedAt: now, NextAttempt: now.Add(24 * time.Hour)}
	if err := store.SaveZoneBoundary(ctx, good); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(border("YOW"), `"YOW"`) {
		t.Fatal("imported boundary did not override the manual border")
	}
	if err := store.UpsertIATABorder(ctx, "YOW", manual); err != nil {
		t.Fatal(err)
	}
	failed := meshmapper.Boundary{IATA: "YOW", URL: good.URL, AttemptedAt: now.Add(time.Hour), NextAttempt: now.Add(2 * time.Hour), LastError: "HTTP 503"}
	if err := store.SaveZoneBoundary(ctx, failed); err != nil {
		t.Fatal(err)
	}
	rows, err := store.ListZoneBoundaries(ctx)
	if err != nil || len(rows) != 1 || !strings.Contains(string(rows[0].Feature), `"YOW"`) || rows[0].ETag != `"v1"` ||
		!rows[0].CheckedAt.Equal(now) || rows[0].LastError != "HTTP 503" || !rows[0].NextAttempt.Equal(failed.NextAttempt) {
		t.Fatal("failure or reseed lost the last good boundary", rows, err)
	}
	if !strings.Contains(border("YOW"), `"YOW"`) {
		t.Fatal("reseeding the manual border undid the override")
	}
	if pruned, err := store.PruneZoneBoundaries(ctx, []string{"YOW"}); err != nil || len(pruned) != 0 {
		t.Fatal("configured import pruned", pruned, err)
	}
	if pruned, err := store.PruneZoneBoundaries(ctx, nil); err != nil || len(pruned) != 1 || pruned[0] != "YOW" {
		t.Fatal(pruned, err)
	}
	if !strings.Contains(border("YOW"), `"file"`) {
		t.Fatal("manual border did not return after pruning")
	}
	list := meshmapper.ZoneList{Country: "CA", Payload: []byte(`{"country":"CA","zones":[]}`), ETag: `"z1"`,
		FetchedAt: now, AttemptedAt: now, NextAttempt: now.Add(24 * time.Hour)}
	if err := store.SaveZoneList(ctx, list); err != nil {
		t.Fatal(err)
	}
	later := now.Add(25 * time.Hour)
	if err := store.SaveZoneList(ctx, meshmapper.ZoneList{Country: "CA", AttemptedAt: later, NextAttempt: later.Add(48 * time.Hour), LastError: "HTTP 429"}); err != nil {
		t.Fatal(err)
	}
	lists, err := store.ListZoneLists(ctx)
	if err != nil || len(lists) != 1 || len(lists[0].Payload) == 0 || lists[0].ETag != `"z1"` || !lists[0].FetchedAt.Equal(now) ||
		lists[0].LastError != "HTTP 429" || !lists[0].NextAttempt.Equal(later.Add(48*time.Hour)) {
		t.Fatal("failed fetch lost the last good zone list", lists, err)
	}
}

func TestMeshMapperRegionsPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	// Real tables inside the rolled-back tx: TEMP copies drop region_iatas' foreign key to iata_codes.
	store := &Store{q: sqlc.New(tx)}
	state := func() map[string]meshmapper.RegionState {
		t.Helper()
		rows, err := store.ListRegionState(ctx)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]meshmapper.RegionState{}
		for _, r := range rows {
			out[r.Slug] = r
		}
		return out
	}
	for _, iata := range []string{"YOW", "YUL"} { // Seed creates config region IATAs first
		if err := store.UpsertIATA(ctx, iata); err != nil {
			t.Fatal(err)
		}
	}
	hand, err := store.UpsertRegion(ctx, "east", "East", "", 3, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SetRegionIATAs(ctx, hand, []string{"YOW", "YUL"}); err != nil {
		t.Fatal(err)
	}
	if err := store.SetRegionIATAs(ctx, hand, []string{"YOW"}); err != nil {
		t.Fatal(err)
	}
	if got := state()["east"]; got.Imported || got.DisplayOrder != 3 || strings.Join(got.IATAs, ",") != "YOW" {
		t.Fatal("configured members not replaced", got)
	}
	lat, lng := 45.5, -75.0
	group := meshmapper.RegionState{Slug: "onqc", Name: "Corridor", DisplayOrder: 4, IATAs: []string{"YOW", "YQB"}, CenterLat: &lat, CenterLng: &lng}
	if saved, err := store.SaveImportedRegion(ctx, group); err != nil || !saved {
		t.Fatal("members Beacon hasn't heard must be created first", saved, err)
	}
	if got := state()["onqc"]; got.CenterLat == nil || *got.CenterLat != lat || *got.CenterLng != lng {
		t.Fatal("center not saved", got)
	}
	details, err := store.ListIATADetails(ctx)
	if err != nil || !slices.ContainsFunc(details, func(d meshmapper.IATADetails) bool { return d.IATA == "YQB" }) {
		t.Fatal("member IATA not created", err)
	}
	group.IATAs = []string{"YQB", "YUL"}
	if saved, err := store.SaveImportedRegion(ctx, group); err != nil || !saved {
		t.Fatal(saved, err)
	}
	if got := state()["onqc"]; !got.Imported || strings.Join(got.IATAs, ",") != "YQB,YUL" {
		t.Fatal("imported region not updated", got)
	}
	if saved, err := store.SaveImportedRegion(ctx, meshmapper.RegionState{Slug: "east", Name: "Group", IATAs: []string{"YYZ"}}); err != nil || saved {
		t.Fatal("imported group replaced a configured region", saved, err)
	}
	if got := state()["east"]; got.Name != "East" || strings.Join(got.IATAs, ",") != "YOW" {
		t.Fatal("configured region changed by a clash", got)
	}
	if _, err := store.UpsertRegion(ctx, "onqc", "Mine", "", 1, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if state()["onqc"].Imported {
		t.Fatal("config did not take over an imported slug")
	}
	if _, err := store.SaveImportedRegion(ctx, meshmapper.RegionState{Slug: "golm", Name: "Lakes", IATAs: []string{"YYZ"}}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertIATADetails(ctx, "YQB", "Quebec City", &lat, &lng); err != nil {
		t.Fatal(err)
	}
	if details, _ := store.ListIATADetails(ctx); !slices.ContainsFunc(details, func(d meshmapper.IATADetails) bool {
		return d.IATA == "YQB" && d.Name == "Quebec City" && d.Lat != nil && *d.Lat == lat
	}) {
		t.Fatal("IATA details not saved")
	}
	if pruned, err := store.PruneImportedRegions(ctx, nil); err != nil || strings.Join(pruned, ",") != "golm" {
		t.Fatal("prune touched configured regions", pruned, err)
	}
}

func TestChannelRegionScopingPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	for _, table := range []string{"regions", "region_iatas", "channels", "channel_iatas", "channel_config_scopes",
		"meshmapper_channel_members", "meshmapper_channel_catalogues"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	store := &Store{q: sqlc.New(tx)}
	for slug, iata := range map[string]string{"east": "YOW", "west": "YYZ"} {
		id, err := store.UpsertRegion(ctx, slug, slug, "", 0, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.SetRegionIATAs(ctx, id, []string{iata}); err != nil {
			t.Fatal(err)
		}
	}
	fp := func(b byte) []byte { return []byte{b, b, b, b, b, b, b, b} }
	for name, b := range map[string]byte{"#mm-yow": 1, "#config-east": 2, "#global": 3, "#toronto": 4} {
		if _, err := store.UpsertChannel(ctx, []byte{b}, fp(b), name, name[1:]); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.UpsertChannelIATA(ctx, []byte{4}, "YOW", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.SetChannelConfigScopes(ctx, [][]byte{fp(2), fp(3)}, []string{"east", ""}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for iata, b := range map[string]byte{"YOW": 1, "YYZ": 4} {
		update := meshmapper.Cache{Payload: []byte(`{}`), CheckedAt: now, AttemptedAt: now, NextAttempt: now.Add(24 * time.Hour)}
		if err := store.SaveChannelCatalogue(ctx, iata, "https://x.meshmapper.net/get_channels.php", update, [][]byte{fp(b)}); err != nil {
			t.Fatal(err)
		}
	}
	names := func(iatas ...string) string {
		t.Helper()
		page, err := store.ListChannels(ctx, 50, nil, iatas, nil, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, c := range page.Items {
			out = append(out, *c.Name)
		}
		slices.Sort(out)
		return strings.Join(out, ",")
	}
	if got := names("YOW"); got != "#config-east,#global,#mm-yow" {
		t.Fatal("YOW channels:", got)
	}
	if got := names("YYZ"); got != "#global,#toronto" {
		t.Fatal("YYZ channels:", got)
	}
	if got := names(); got != "#config-east,#global,#mm-yow,#toronto" {
		t.Fatal("unfiltered channels:", got)
	}
	// A 304 keeps membership; a new payload replaces it.
	if err := store.SaveChannelCatalogue(ctx, "YOW", "https://x.meshmapper.net/get_channels.php", meshmapper.Cache{AttemptedAt: now, NextAttempt: now}, nil); err != nil {
		t.Fatal(err)
	}
	if got := names("YOW"); got != "#config-east,#global,#mm-yow" {
		t.Fatal("304 dropped membership:", got)
	}
	if err := store.SaveChannelCatalogue(ctx, "YOW", "https://x.meshmapper.net/get_channels.php", meshmapper.Cache{Payload: []byte(`{}`), AttemptedAt: now, NextAttempt: now}, [][]byte{}); err != nil {
		t.Fatal(err)
	}
	if got := names("YOW"); got != "#config-east,#global" {
		t.Fatal("empty list kept membership:", got)
	}
	cats, err := store.ListChannelCatalogues(ctx)
	if err != nil || len(cats) != 2 {
		t.Fatal(cats, err)
	}
	if err := store.ClearChannelMembers(ctx); err != nil || names("YYZ") != "#global" {
		t.Fatal("members not cleared", err)
	}
}
