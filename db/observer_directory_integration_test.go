// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

func TestObserverDirectoryStablePagesPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, `INSERT INTO iata_codes(iata) VALUES ('YVR'),('YYJ');
 INSERT INTO observers(id,public_key,display_name,observer_type,last_iata,last_seen) SELECT lpad(to_hex(i),32,'0')::uuid,int4send(i),'Observer '||lpad(i::text,3,'0'),CASE WHEN i%2=0 THEN 'type-a' ELSE 'type-b' END,'YVR',now() FROM generate_series(1,205)i;
 INSERT INTO analytics_rollup_hours(hour,status) VALUES ('2026-01-01 00:00:00+00','complete');
 INSERT INTO analytics_hourly_observer_identity(hour,iata,observer_id,observation_count) SELECT '2026-01-01 00:00:00+00','YVR',id,CASE WHEN display_name IN ('Observer 001','Observer 002') THEN 100 ELSE 1 END FROM observers WHERE display_name <> 'Observer 205';`)
	if err != nil {
		t.Fatal(err)
	}
	store := New(pool, 0, 0)
	q := api.ObserverDirectoryQuery{IATAs: []string{"YVR"}, Sort: "traffic", Since: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), Until: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC).UnixMilli(), Limit: 2}
	first, err := store.ListObserverDirectory(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 2 || *first.Items[0].DisplayName != "Observer 001" || *first.Items[1].DisplayName != "Observer 002" || !first.HasMore || *first.MaxObservationCount != 100 {
		t.Fatalf("first %+v", first)
	}
	if len(first.ObserverTypes) != 2 {
		t.Fatalf("facets %+v", first.ObserverTypes)
	}
	_, err = pool.Exec(ctx, `UPDATE analytics_hourly_observer_identity SET observation_count=900; UPDATE observers SET display_name='Changed',last_iata='YYJ'; DELETE FROM observers WHERE public_key=int4send(4);`)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	page := first
	for {
		for _, item := range page.Items {
			if seen[item.ID.String()] {
				t.Fatal("duplicate observer")
			}
			seen[item.ID.String()] = true
			if item.IATA != "YVR" || *item.DisplayName == "Changed" {
				t.Fatal("metadata moved between pages")
			}
		}
		if !page.HasMore {
			break
		}
		page, err = store.ListObserverDirectory(ctx, api.ObserverDirectoryQuery{Snapshot: first.Snapshot, Cursor: *page.NextCursor, Limit: 37})
		if err != nil {
			t.Fatal(err)
		}
		if *page.MaxObservationCount != 100 || page.WindowEnd != first.WindowEnd {
			t.Fatal("snapshot metrics changed")
		}
	}
	if len(seen) != 205 {
		t.Fatalf("got %d observers", len(seen))
	}
	if *page.Items[len(page.Items)-1].ObservationCount != 0 {
		t.Fatal("missing explicit zero")
	}
	if _, err := pool.Exec(ctx, `UPDATE observer_directory_snapshots SET expires_at=now()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	_, err = store.ListObserverDirectory(ctx, api.ObserverDirectoryQuery{Snapshot: first.Snapshot, Limit: 10})
	if !errors.Is(err, api.ErrDirectoryExpired) {
		t.Fatalf("expiry: %v", err)
	}
}

func TestObserverDirectoryFiltersAndCoveragePostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, `INSERT INTO iata_codes(iata) VALUES ('YVR'),('YYJ');
 INSERT INTO observers(id,public_key,display_name,observer_type,last_iata,last_seen,last_status_at) VALUES
 ('00000000-0000-0000-0000-000000000001','\x01','Zulu','a','YVR',now(),now()),
 ('00000000-0000-0000-0000-000000000002','\x02','alpha','b','YVR',now()-interval '1 hour',now()-interval '1 hour'),
 ('00000000-0000-0000-0000-000000000003','\x03','alpha','a','YVR',now(),now()),
 ('00000000-0000-0000-0000-000000000004','\x04','Other','b','YYJ',now(),now());
 INSERT INTO observer_brokers(observer_id,broker_name) VALUES ('00000000-0000-0000-0000-000000000001','mqtt1');
 INSERT INTO transport_scopes(id,name,transport_key,key_fingerprint) VALUES (1,'#test','\x01','\x01');
 INSERT INTO observer_scopes(observer_id,scope_id) VALUES ('00000000-0000-0000-0000-000000000001',1);
 INSERT INTO analytics_rollup_hours(hour,status) VALUES ('2026-01-01 00:00:00+00','complete');
 INSERT INTO analytics_hourly_observer_identity(hour,iata,observer_id,observation_count) VALUES
 ('2026-01-01 00:00:00+00','YVR','00000000-0000-0000-0000-000000000001',5),
 ('2026-01-01 00:00:00+00','YYJ','00000000-0000-0000-0000-000000000001',999),
 ('2026-01-01 00:00:00+00','YVR','00000000-0000-0000-0000-000000000002',8),
 ('2025-12-31 00:00:00+00','YVR','00000000-0000-0000-0000-000000000003',100);`)
	if err != nil {
		t.Fatal(err)
	}
	store := New(pool, 0, 0)
	base := api.ObserverDirectoryQuery{IATAs: []string{"YVR"}, Sort: "traffic", Since: 1767225600000, Until: 1767229200000, Limit: 1}
	cases := []struct {
		name  string
		apply func(*api.ObserverDirectoryQuery)
		ids   []byte
	}{
		{"traffic", func(q *api.ObserverDirectoryQuery) {}, []byte{2, 1, 3}},
		{"name ties", func(q *api.ObserverDirectoryQuery) { q.Sort = "name" }, []byte{2, 3, 1}},
		{"name filter", func(q *api.ObserverDirectoryQuery) { q.Name = "zUL" }, []byte{1}},
		{"type", func(q *api.ObserverDirectoryQuery) { q.Type = "a" }, []byte{1, 3}},
		{"broker", func(q *api.ObserverDirectoryQuery) { q.Broker = "mqtt1" }, []byte{1}},
		{"scope", func(q *api.ObserverDirectoryQuery) { q.Scope = "#test" }, []byte{1}},
		{"status", func(q *api.ObserverDirectoryQuery) { q.Status = "offline" }, []byte{2}},
		{"empty region", func(q *api.ObserverDirectoryQuery) { q.MatchNone = true }, []byte{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := base
			tc.apply(&q)
			page, err := store.ListObserverDirectory(ctx, q)
			if err != nil {
				t.Fatal(err)
			}
			var ids []byte
			for {
				for _, o := range page.Items {
					ids = append(ids, o.ID[15])
					if o.ID[15] == 1 && *o.ObservationCount != 5 {
						t.Fatal("count included other IATA")
					}
					if o.ID[15] == 3 && *o.ObservationCount != 0 {
						t.Fatal("count included other hour")
					}
				}
				if !page.HasMore {
					break
				}
				page, err = New(pool, 0, 0).ListObserverDirectory(ctx, api.ObserverDirectoryQuery{Snapshot: page.Snapshot, Cursor: *page.NextCursor, Limit: 1})
				if err != nil {
					t.Fatal(err)
				}
			}
			if !slices.Equal(ids, tc.ids) {
				t.Fatalf("ids %v want %v", ids, tc.ids)
			}
			if tc.name == "type" && len(page.ObserverTypes) != 2 {
				t.Fatal("type filter hid alternatives")
			}
			if tc.name == "name ties" && *page.MaxObservationCount != 8 {
				t.Fatal("bar maximum followed page instead of snapshot")
			}
		})
	}
	for _, status := range []string{"partial", "missing"} {
		if _, err := pool.Exec(ctx, `DELETE FROM observer_directory_snapshots`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE analytics_rollup_hours SET status=$1`, status); err != nil {
			t.Fatal(err)
		}
		page, err := store.ListObserverDirectory(ctx, base)
		if err != nil {
			t.Fatal(err)
		}
		want := "partial"
		if status == "missing" {
			want = "unavailable"
		}
		if page.Coverage.Status != want || page.EffectiveSort != "name" || page.MaxObservationCount != nil || page.Items[0].ObservationCount != nil {
			t.Fatalf("unknown analytics represented as zero: %+v", page)
		}
	}
}

func TestObserverDirectoryBoundsPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := New(pool, 0, 0)
	q := api.ObserverDirectoryQuery{Sort: "traffic", Since: 1767225600000, Until: 1767229200000, Limit: 1}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7261930284521)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ListObserverDirectory(ctx, q); !errors.Is(err, api.ErrDirectoryBusy) {
		t.Fatalf("creation lock: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	first, err := store.ListObserverDirectory(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	q.Limit = 50
	reused, err := store.ListObserverDirectory(ctx, q)
	if err != nil || reused.Snapshot != first.Snapshot {
		t.Fatalf("reuse %+v: %v", reused, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO observer_directory_snapshots(id,query_key,metadata,items) SELECT lpad(to_hex(i),32,'0')::uuid,int4send(i),'{}','[]' FROM generate_series(1,63)i`); err != nil {
		t.Fatal(err)
	}
	q.Name = "beyond old capacity"
	if _, err := store.ListObserverDirectory(ctx, q); err != nil {
		t.Fatalf("65th snapshot: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO observer_directory_snapshots(id,query_key,metadata,items) SELECT lpad(to_hex(i),32,'0')::uuid,int4send(i),'{}','[]' FROM generate_series(64,1022)i`); err != nil {
		t.Fatal(err)
	}
	q.Name = "new criteria"
	if _, err := store.ListObserverDirectory(ctx, q); !errors.Is(err, api.ErrDirectoryBusy) {
		t.Fatalf("capacity: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE observer_directory_snapshots SET expires_at=now()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	fresh, err := store.ListObserverDirectory(ctx, q)
	if err != nil || fresh.Snapshot == first.Snapshot {
		t.Fatalf("prune %+v: %v", fresh, err)
	}
}

func TestObserverDirectoryStorageBudgetPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	// Keep fixture compression from hiding the storage boundary.
	if _, err := pool.Exec(ctx, `ALTER TABLE observer_directory_snapshots ALTER COLUMN items SET STORAGE EXTERNAL;
 INSERT INTO observer_directory_snapshots(id,query_key,metadata,items)
 SELECT lpad(to_hex(i),32,'0')::uuid,int4send(i),'{}',jsonb_build_array(repeat('x',15*1024*1024)) FROM generate_series(1,9)i`); err != nil {
		t.Fatal(err)
	}
	q := api.ObserverDirectoryQuery{Sort: "traffic", Since: 1767225600000, Until: 1767229200000, Limit: 1}
	if _, err := New(pool, 0, 0).ListObserverDirectory(ctx, q); !errors.Is(err, api.ErrDirectoryBusy) {
		t.Fatalf("storage budget: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM observer_directory_snapshots`).Scan(&n); err != nil || n != 9 {
		t.Fatalf("rejected snapshot persisted: count=%d err=%v", n, err)
	}
}
