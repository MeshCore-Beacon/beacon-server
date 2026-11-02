// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
)

// TestPacedFillsEarnTrustPostgres boots both read models over data an older binary left without read-model rows,
// with a small duty, a short floor and tiny slices and batches: the backfills still complete, every page equals the
// observations, and trust is earned.
func TestPacedFillsEarnTrustPostgres(t *testing.T) {
	ctx, pool := migratedPool(t)
	pace := db.FillPacing{Duty: 0.25, MinPause: time.Millisecond, PacketSitesSlice: 7, CmiBatch: 3}
	maint := db.New(pool, pool, 0, 0, db.WithFillPacing(pace))
	if got := maint.FillPacing(); got != pace {
		t.Fatalf("store pacing %+v", got)
	}

	// packet_sites: 60 packets heard at one to three sites without their site rows.
	observers := psPrepare(t, ctx, pool)
	t0 := time.Now().Add(-time.Hour)
	for i := range 60 {
		psHeard(t, ctx, maint, pool, false, fmt.Sprintf("p%02d", i), t0.Add(time.Duration(i)*time.Second), observers, psTestSites[:1+i%3]...)
	}
	if missing := psMissingMoved(t, ctx, pool); missing != 120 {
		t.Fatalf("missing site rows %d, want 120", missing)
	}

	// cmi: 20 group-text messages stored without their site rows.
	if _, err := pool.Exec(ctx, `SELECT setval('channel_messages_id_seq', 3000000000);
INSERT INTO iata_codes (iata) VALUES ('YUL'), ('YOW') ON CONFLICT DO NOTHING;
INSERT INTO channels (id, channel_hash, name) VALUES (7, '\x11', 'ch7')`); err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		hash := []byte(fmt.Sprintf("m%02d", i))
		if _, err := maint.UpsertPacket(ctx, ingest.UpsertPacketParams{PacketHash: hash, PayloadType: 5, RouteType: 1, RawHeader: []byte{0x15}, RawPayload: hash}); err != nil {
			t.Fatal(err)
		}
		for _, site := range []string{"AAA", "BBB"}[:1+i%2] {
			psHeard(t, ctx, maint, pool, true, string(hash), t0.Add(time.Duration(100+i)*time.Second), observers, site)
		}
		if _, err := pool.Exec(ctx, "INSERT INTO channel_messages (channel_id, packet_hash, sent_at) VALUES (7, $1, now())", hash); err != nil {
			t.Fatal(err)
		}
	}
	if missing := missingPairs(t, ctx, pool); missing != 30 {
		t.Fatalf("missing cmi pairs %d, want 30", missing)
	}

	psTrust := db.NewTrust("packet_sites", true, db.PacketSitesTrustPolicy)
	cmiTrust := db.NewTrust("cmi", true, db.CmiTrustPolicy)
	psBootWithin(t, ctx, psJobsFor(maint, psTrust), time.Minute)
	bootWithin(t, ctx, cmiJobsFor(maint, cmiTrust), time.Minute)

	if n := psMissingMoved(t, ctx, pool); n != 0 {
		t.Fatalf("%d site rows missing or moved after the paced backfill", n)
	}
	if n := missingPairs(t, ctx, pool); n != 0 {
		t.Fatalf("%d cmi pairs missing after the paced backfill", n)
	}
	var psState db.PacketSitesState
	var cmiState db.CmiState
	if _, err := maint.LoadState(ctx, "packet_sites", &psState); err != nil || !psState.Complete || psState.FirstVerifiedAt == nil {
		t.Fatalf("packet_sites state %+v, %v", psState, err)
	}
	if _, err := maint.LoadState(ctx, "cmi", &cmiState); err != nil || !cmiState.Complete || cmiState.FirstVerifiedAt == nil {
		t.Fatalf("cmi state %+v, %v", cmiState, err)
	}

	served := db.New(pool, nil, 0, 0, db.WithReadModels(&db.ReadModels{PacketSites: psTrust, Cmi: cmiTrust}))
	for _, sites := range [][]string{{"AAA"}, {"BBB"}, {"CCC"}, psTestSites} {
		got, want := psPage(t, ctx, served, sites), psTruth(t, ctx, pool, sites, 200)
		if strings.Join(got, ",") != strings.Join(want, ",") || len(got) == 0 {
			t.Fatalf("sites %v after the paced backfill:\n got %v\nwant %v", sites, got, want)
		}
	}
	original := db.New(pool, nil, 0, 0)
	ch := int32(7)
	for _, sites := range [][]string{{"AAA"}, {"BBB"}, {"AAA", "BBB"}} {
		got, err := served.ListChannelMessages(ctx, &ch, time.Time{}, 50, sites, "", 0)
		if err != nil {
			t.Fatal(err)
		}
		want, err := original.OriginalChannelMessages(ctx, &ch, time.Time{}, 50, sites, "", 0)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) || len(got.Items) == 0 {
			t.Fatalf("%v: %+v, the original statements %+v", sites, got, want)
		}
	}
}
