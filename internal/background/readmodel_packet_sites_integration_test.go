// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var psTestSites = []string{"AAA", "BBB", "CCC", "NEW"}

func psJobsFor(store *db.Store, tr *db.Trust) *PacketSitesJobs {
	return &PacketSitesJobs{
		Begin: func(ctx context.Context) (PacketSitesSession, bool, error) {
			s, ok, err := store.BeginPacketSites(ctx)
			if s == nil {
				return nil, ok, err
			}
			return s, ok, err
		},
		Wait:     func(context.Context) error { return nil },
		Trust:    tr,
		Interval: 10 * time.Millisecond,
	}
}

func psBootWithin(t *testing.T, ctx context.Context, j *PacketSitesJobs, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() { j.Boot(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("boot did not finish")
	}
	if !j.Trust.Trusted() {
		t.Fatal("not trusted after the boot")
	}
}

// psPrepare adds the sites and one observer per site and moves observation ids above 3,000,000,000.
func psPrepare(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]uuid.UUID {
	t.Helper()
	if _, err := pool.Exec(ctx, "SELECT setval('packet_observations_id_seq', 3000000000)"); err != nil {
		t.Fatal(err)
	}
	observers := map[string]uuid.UUID{}
	for _, site := range psTestSites {
		observers[site] = uuid.New()
		if _, err := pool.Exec(ctx, "INSERT INTO iata_codes (iata) VALUES ($1) ON CONFLICT DO NOTHING", site); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, "INSERT INTO observers (id, public_key) VALUES ($1, $2)", observers[site], []byte(site)); err != nil {
			t.Fatal(err)
		}
	}
	return observers
}

// psHeard stores packet hash heard at sites at at; sited goes through the store's writer, otherwise through the
// statement an older binary runs.
func psHeard(t *testing.T, ctx context.Context, store *db.Store, pool *pgxpool.Pool, sited bool, hash string, at time.Time, observers map[string]uuid.UUID, sites ...string) {
	t.Helper()
	if _, err := store.UpsertPacket(ctx, ingest.UpsertPacketParams{PacketHash: []byte(hash), PayloadType: 4, RouteType: 1, RawHeader: []byte{0x11}, RawPayload: []byte(hash)}); err != nil {
		t.Fatal(err)
	}
	for _, site := range sites {
		o := ingest.InsertObservationParams{PacketHash: []byte(hash), ObserverID: observers[site], IATA: site, HeardAt: at, HashSize: 1, PayloadType: 4}
		if sited {
			if _, _, err := store.InsertObservation(ctx, o); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, err := sqlc.New(pool).InsertObservationUnsited(ctx, sqlc.InsertObservationUnsitedParams{
			PacketHash: o.PacketHash, ObserverID: o.ObserverID, Iata: o.IATA, HeardAt: pgtype.Timestamptz{Time: at, Valid: true}, HashSize: 1, PayloadType: &o.PayloadType,
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// psTruth reads a region page's packet hashes straight from the observations.
func psTruth(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sites []string, limit int) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT encode(packet_hash, 'hex') FROM packet_observations WHERE iata = ANY($1::bpchar[])
GROUP BY packet_hash ORDER BY max(heard_at) DESC, packet_hash DESC LIMIT $2`, sites, limit)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func psPage(t *testing.T, ctx context.Context, store *db.Store, sites []string) []string {
	t.Helper()
	page, err := store.ListPackets(ctx, nil, nil, sites, nil, time.Time{}, time.Time{}, 0, nil, 200)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, it := range page.Items {
		out = append(out, it.PacketHash)
	}
	return out
}

func psMissingMoved(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	row, err := sqlc.New(pool).CheckPacketSitesSlice(ctx, sqlc.CheckPacketSitesSliceParams{AfterID: 0, ThroughID: 1 << 62})
	if err != nil {
		t.Fatal(err)
	}
	if row.Extra != 0 {
		t.Fatalf("extra rows %+v", row)
	}
	return row.Missing + row.Wrong
}

func TestPacketSitesRollbackRollForwardPostgres(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		ctx, pool := migratedPool(t)
		psBootWithin(t, ctx, psJobsFor(db.New(pool, pool, 0, 0), db.NewTrust("packet_sites", true, db.PacketSitesTrustPolicy)), 30*time.Second)
	})

	ctx, pool := migratedPool(t)
	maint := db.New(pool, pool, 0, 0)
	observers := psPrepare(t, ctx, pool)
	t0 := time.Now().Add(-time.Hour)
	for i := range 40 {
		psHeard(t, ctx, maint, pool, true, fmt.Sprintf("p%02d", i), t0.Add(time.Duration(i)*time.Second), observers, psTestSites[:1+i%3]...)
	}
	tr := db.NewTrust("packet_sites", true, db.PacketSitesTrustPolicy)
	psBootWithin(t, ctx, psJobsFor(maint, tr), 30*time.Second)
	rec := &statementNames{DBTX: pool}
	trusted := db.New(rec, nil, 0, 0, db.WithReadModels(&db.ReadModels{PacketSites: tr}))
	psPage(t, ctx, trusted, []string{"AAA"})
	if got := strings.Join(rec.names, ","); !strings.HasPrefix(got, "ListPacketsBySite") {
		t.Fatalf("trusted store ran %s", got)
	}

	// An older binary while this one was down: new packets, new sites, later hearings, without site rows.
	for i := range 100 {
		psHeard(t, ctx, maint, pool, false, fmt.Sprintf("old%03d", i), t0.Add(time.Duration(40+i)*time.Second), observers, psTestSites[i%4])
	}
	for i := range 20 {
		psHeard(t, ctx, maint, pool, false, fmt.Sprintf("p%02d", i), t0.Add(time.Duration(200+i)*time.Second), observers, "NEW")
	}
	missing := psMissingMoved(t, ctx, pool)
	if missing != 120 {
		t.Fatalf("the older binary left %d rows missing or moved, want 120", missing)
	}

	// The next boot: untrusted until its repair finishes, then every page equals the observations.
	tr2 := db.NewTrust("packet_sites", true, db.PacketSitesTrustPolicy)
	served := db.New(rec, nil, 0, 0, db.WithReadModels(&db.ReadModels{PacketSites: tr2}))
	rec.names = nil
	if _, err := served.ListPackets(ctx, nil, nil, []string{"AAA"}, nil, time.Time{}, time.Time{}, 0, nil, 200); !errors.Is(err, api.ErrPagesNotReady) || len(rec.names) != 0 {
		t.Fatalf("before the boot repair: err %v, statements %v; want ErrPagesNotReady and none", err, rec.names)
	}
	before := repairedRows("packet_sites")
	j := psJobsFor(maint, tr2)
	psBootWithin(t, ctx, j, 30*time.Second)
	if got := repairedRows("packet_sites") - before; got != float64(missing) {
		t.Fatalf("repaired counter grew by %v, want %d", got, missing)
	}
	for _, sites := range [][]string{{"AAA"}, {"BBB"}, {"CCC"}, {"NEW"}, psTestSites} {
		got, want := psPage(t, ctx, served, sites), psTruth(t, ctx, pool, sites, 200)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("sites %v after the repair:\n got %v\nwant %v", sites, got, want)
		}
	}
	before = repairedRows("packet_sites")
	if err := j.Task().Run(ctx); err != nil || repairedRows("packet_sites") != before || !tr2.Trusted() {
		t.Fatalf("the next run: %v, repaired +%v, trusted %v", err, repairedRows("packet_sites")-before, tr2.Trusted())
	}
}

func TestPacketSitesTrustDropsOnBypassPostgres(t *testing.T) {
	ctx, pool := migratedPool(t)
	maint := db.New(pool, pool, 0, 0)
	observers := psPrepare(t, ctx, pool)
	for i := range 10 {
		psHeard(t, ctx, maint, pool, true, fmt.Sprintf("p%02d", i), time.Now(), observers, "AAA")
	}
	tr := db.NewTrust("packet_sites", true, db.PacketSitesTrustPolicy)
	j := psJobsFor(maint, tr)
	psBootWithin(t, ctx, j, 30*time.Second)
	for i := range 5 {
		psHeard(t, ctx, maint, pool, false, fmt.Sprintf("x%02d", i), time.Now(), observers, "BBB")
	}
	before := repairedRows("packet_sites")
	task := j.Task()
	if err := task.Run(ctx); err != nil || tr.Trusted() || repairedRows("packet_sites")-before != 5 {
		t.Fatalf("bypass run: %v, trusted %v, repaired +%v", err, tr.Trusted(), repairedRows("packet_sites")-before)
	}
	task.Run(ctx) //nolint:errcheck
	if tr.Trusted() {
		t.Fatal("trusted after one clean run")
	}
	if err := task.Run(ctx); err != nil || !tr.Trusted() {
		t.Fatalf("not trusted after two clean runs: %v", err)
	}
}

func TestPacketSitesReadOnlyTrustPostgres(t *testing.T) {
	ctx, pool := migratedPool(t)
	maint := db.New(pool, pool, 0, 0)
	observers := psPrepare(t, ctx, pool)
	for i := range 10 {
		psHeard(t, ctx, maint, pool, true, fmt.Sprintf("p%02d", i), time.Now(), observers, "AAA", "BBB")
	}
	cfg := pool.Config().Copy()
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	roPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(roPool.Close)
	reader := db.New(roPool, nil, 0, 0)

	// The writer has not verified it yet.
	j := psJobsFor(maint, db.NewTrust("packet_sites_writer", true, db.PacketSitesTrustPolicy))
	s, _, err := maint.BeginPacketSites(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Backfill(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	s.Close(ctx)
	tr := db.NewTrust("packet_sites", true, db.PacketSitesTrustPolicy)
	PacketSitesReadOnlyTrust(ctx, reader.CheckPacketSitesReadOnly, tr)
	if tr.Trusted() {
		t.Fatal("trusted before the first verification")
	}
	psBootWithin(t, ctx, j, 30*time.Second)
	PacketSitesReadOnlyTrust(ctx, reader.CheckPacketSitesReadOnly, tr)
	if !tr.Trusted() {
		t.Fatal("not trusted with a complete, verified and clean table")
	}
	// A row lost from the newest observations: the next check drops trust.
	if _, err := pool.Exec(ctx, "DELETE FROM packet_sites WHERE packet_hash = $1", []byte("p09")); err != nil {
		t.Fatal(err)
	}
	PacketSitesReadOnlyTrust(ctx, reader.CheckPacketSitesReadOnly, tr)
	if tr.Trusted() {
		t.Fatal("trusted with a missing row")
	}
}
