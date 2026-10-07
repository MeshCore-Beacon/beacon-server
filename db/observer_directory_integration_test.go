// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
)

func TestObserverDirectoryPagesPostgres(t *testing.T) {
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
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	store := &Store{q: sqlc.New(tx)}
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
		q.Cursor = *page.NextCursor
		q.Limit = 37
		page, err = store.ListObserverDirectory(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		if *page.MaxObservationCount != 100 || page.WindowEnd != first.WindowEnd {
			t.Fatal("unchanged data produced different metrics")
		}
	}
	if len(seen) != 205 {
		t.Fatalf("got %d observers", len(seen))
	}
	if *page.Items[len(page.Items)-1].ObservationCount != 0 {
		t.Fatal("missing explicit zero")
	}
	q.Cursor = 0
	if _, err := pool.Exec(ctx, `UPDATE analytics_hourly_observer_identity SET observation_count=900; UPDATE observers SET display_name='Changed'`); err != nil {
		t.Fatal(err)
	}
	refreshed, err := store.ListObserverDirectory(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if *refreshed.MaxObservationCount != 900 || *refreshed.Items[0].DisplayName != "Changed" {
		t.Fatal("directory did not reflect current data")
	}
	q.Cursor = 1000
	empty, err := store.ListObserverDirectory(ctx, q)
	if err != nil || len(empty.Items) != 0 || empty.HasMore || empty.NextCursor != nil {
		t.Fatalf("terminal page %+v, %v", empty, err)
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
				q.Cursor = *page.NextCursor
				page, err = New(pool, 0, 0).ListObserverDirectory(ctx, q)
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
				t.Fatal("bar maximum followed page instead of full result")
			}
		})
	}
	for _, status := range []string{"partial", "missing"} {
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
		if page.Coverage.Status != want || page.EffectiveSort != "traffic" || page.MaxObservationCount == nil || *page.MaxObservationCount != 0 || page.Items[0].ObservationCount == nil || *page.Items[0].ObservationCount != 0 {
			t.Fatalf("incomplete history suppressed numeric counts or traffic sorting: %+v", page)
		}
	}
}

func directoryFixture(t *testing.T) (context.Context, pgx.Tx, *Store) {
	t.Helper()
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	if _, err := tx.Exec(ctx, `INSERT INTO observers(id,public_key,display_name,observer_type,last_iata,last_seen,last_status_at) VALUES
 ('00000000-0000-0000-0000-000000000001','\x01','Zulu','a','YVR',now(),now()),
 ('00000000-0000-0000-0000-000000000002','\x02','alpha','b','YVR',now()-interval '1 hour',now()-interval '1 hour'),
 ('00000000-0000-0000-0000-000000000003','\x03','alpha','a','YVR',now(),now()),
 ('00000000-0000-0000-0000-000000000004','\x04','Other','b','YYJ',now(),now());
 INSERT INTO observer_brokers(observer_id,broker_name) VALUES ('00000000-0000-0000-0000-000000000001','mqtt1');
 INSERT INTO transport_scopes(id,name,transport_key,key_fingerprint) VALUES (1,'#test','\x01','\x01');
 INSERT INTO observer_scopes(observer_id,scope_id) VALUES ('00000000-0000-0000-0000-000000000001',1);`); err != nil {
		t.Fatal(err)
	}
	return ctx, tx, &Store{q: sqlc.New(tx)}
}

func directoryObservation(t *testing.T, ctx context.Context, tx pgx.Tx, observer int, iata string, at time.Time) {
	t.Helper()
	_, err := tx.Exec(ctx, `WITH packet AS (
 INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at)
 VALUES (decode(md5(gen_random_uuid()::text),'hex'),4,0,1,'\x00','\x00',$1,$1) RETURNING packet_hash
 ) INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count)
 SELECT packet_hash,$2,$3,$1,0,1,0 FROM packet`, at, fmt.Sprintf("00000000-0000-0000-0000-%012d", observer), iata)
	if err != nil {
		t.Fatal(err)
	}
}

func TestObserverDirectoryMixedHistoryPostgres(t *testing.T) {
	ctx, tx, store := directoryFixture(t)
	_, err := tx.Exec(ctx, `
 INSERT INTO analytics_rollup_hours(hour,status) VALUES
 ('2026-01-01 00:00:00+00','complete'), ('2026-01-01 01:00:00+00','complete'),
 ('2026-01-01 02:00:00+00','partial'), ('2026-01-01 03:00:00+00','missing'),
 ('2026-01-01 04:00:00+00','complete');
 INSERT INTO analytics_hourly_observer_identity(hour,iata,observer_id,observation_count) VALUES
 ('2026-01-01 00:00:00+00','YVR','00000000-0000-0000-0000-000000000001',900),
 ('2026-01-01 01:00:00+00','YVR','00000000-0000-0000-0000-000000000001',10),
 ('2026-01-01 01:00:00+00','YVR','00000000-0000-0000-0000-000000000002',12),
 ('2026-01-01 02:00:00+00','YVR','00000000-0000-0000-0000-000000000001',900),
 ('2026-01-01 03:00:00+00','YVR','00000000-0000-0000-0000-000000000001',900),
 ('2026-01-01 04:00:00+00','YVR','00000000-0000-0000-0000-000000000001',900);`)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		id       int
		iata, at string
	}{
		{1, "YVR", "00:30:00.122"}, {1, "YVR", "00:30:00.123"}, {2, "YVR", "00:59:59.999"},
		{1, "YVR", "01:30:00"}, {2, "YVR", "01:35:00"},
		{1, "YVR", "02:15:00"}, {1, "YVR", "02:20:00"}, {1, "YYJ", "02:25:00"},
		{2, "YVR", "03:15:00"}, {4, "YVR", "03:20:00"},
		{1, "YVR", "04:20:00.455"}, {1, "YVR", "04:20:00.456"}, {1, "YVR", "05:00:00"},
	} {
		at, err := time.Parse(time.RFC3339Nano, "2026-01-01T"+row.at+"Z")
		if err != nil {
			t.Fatal(err)
		}
		directoryObservation(t, ctx, tx, row.id, row.iata, at)
	}
	since := time.Date(2026, 1, 1, 0, 30, 0, 123000000, time.UTC).UnixMilli()
	until := time.Date(2026, 1, 1, 4, 20, 0, 456000000, time.UTC).UnixMilli()
	base := api.ObserverDirectoryQuery{IATAs: []string{"YVR"}, Sort: "traffic", Since: since, Until: until, Limit: 1}
	for _, tc := range []struct {
		name    string
		apply   func(*api.ObserverDirectoryQuery)
		ids     []byte
		maximum int64
	}{
		{"traffic", func(*api.ObserverDirectoryQuery) {}, []byte{2, 1, 3}, 14},
		{"name", func(q *api.ObserverDirectoryQuery) { q.Sort = "name" }, []byte{2, 3, 1}, 14},
		{"type", func(q *api.ObserverDirectoryQuery) { q.Type = "a" }, []byte{1, 3}, 14},
		{"broker", func(q *api.ObserverDirectoryQuery) { q.Broker = "mqtt1" }, []byte{1}, 14},
		{"status", func(q *api.ObserverDirectoryQuery) { q.Status = "offline" }, []byte{2}, 14},
		{"name filter", func(q *api.ObserverDirectoryQuery) { q.Name = "ALP" }, []byte{2, 3}, 14},
		{"scope", func(q *api.ObserverDirectoryQuery) { q.Scope = "#test" }, []byte{1}, 14},
		{"all IATAs", func(q *api.ObserverDirectoryQuery) { q.IATAs = nil }, []byte{1, 2, 4, 3}, 15},
		{"multiple IATAs", func(q *api.ObserverDirectoryQuery) { q.IATAs = []string{"YVR", "YYJ"} }, []byte{1, 2, 4, 3}, 15},
		{"empty region", func(q *api.ObserverDirectoryQuery) { q.MatchNone = true }, nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := base
			tc.apply(&q)
			var ids []byte
			for {
				page, err := store.ListObserverDirectory(ctx, q)
				if err != nil {
					t.Fatal(err)
				}
				if page.MaxObservationCount == nil || *page.MaxObservationCount != tc.maximum || page.EffectiveSort != q.Sort || page.WindowStart != since || page.WindowEnd != until {
					t.Fatalf("page metrics %+v", page)
				}
				if page.Coverage.Status != "partial" || page.Coverage.ExpectedHours != 5 || page.Coverage.CompleteHours != 1 {
					t.Fatalf("coverage %+v", page.Coverage)
				}
				for _, item := range page.Items {
					ids = append(ids, item.ID[15])
					want := int64(14)
					if item.ID[15] == 3 {
						want = 0
					}
					if item.ID[15] == 4 {
						want = 1
					}
					if item.ID[15] == 1 && tc.maximum == 15 {
						want = 15
					}
					if item.ObservationCount == nil || *item.ObservationCount != want {
						t.Fatalf("observer %d count %v, want %d", item.ID[15], item.ObservationCount, want)
					}
				}
				if !page.HasMore {
					if page.NextCursor != nil {
						t.Fatal("terminal page has cursor")
					}
					break
				}
				if page.NextCursor == nil || *page.NextCursor <= q.Cursor {
					t.Fatal("cursor did not advance")
				}
				q.Cursor = *page.NextCursor
			}
			if !slices.Equal(ids, tc.ids) {
				t.Fatalf("ids %v, want %v", ids, tc.ids)
			}
		})
	}
	// Raw retention removes both the first boundary slice and the rolled hour's raw rows.
	if _, err := tx.Exec(ctx, `DELETE FROM packet_observations WHERE heard_at < '2026-01-01 02:00:00+00'; UPDATE analytics_raw_state SET raw_deleted_before='2026-01-01 01:30:00+00'`); err != nil {
		t.Fatal(err)
	}
	q := base
	q.Limit = 10
	page, err := store.ListObserverDirectory(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 3 || page.Items[0].ObservationCount == nil || *page.Items[0].ObservationCount != 13 || page.Items[1].ObservationCount == nil || *page.Items[1].ObservationCount != 13 {
		t.Fatalf("retention lost rolled history or included an out-of-window bucket: %+v", page)
	}
}

func TestObserverDirectoryRawStartupAndZeroPostgres(t *testing.T) {
	ctx, tx, store := directoryFixture(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	q := api.ObserverDirectoryQuery{IATAs: []string{"YVR"}, Sort: "traffic", Since: now.Add(-7 * 24 * time.Hour).UnixMilli(), Until: now.UnixMilli(), Limit: 10}
	page, err := store.ListObserverDirectory(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if page.Coverage.Status != "unavailable" || page.EffectiveSort != "traffic" || page.MaxObservationCount == nil || *page.MaxObservationCount != 0 || len(page.Items) != 3 {
		t.Fatalf("empty history: %+v", page)
	}
	for _, item := range page.Items {
		if item.ObservationCount == nil || *item.ObservationCount != 0 {
			t.Fatal("no history must return numeric zero")
		}
	}
	directoryObservation(t, ctx, tx, 1, "YVR", now.Add(-6*24*time.Hour))
	directoryObservation(t, ctx, tx, 1, "YVR", now.Add(-time.Millisecond))
	directoryObservation(t, ctx, tx, 1, "YVR", now.Add(-7*24*time.Hour-time.Millisecond))
	directoryObservation(t, ctx, tx, 1, "YVR", now)
	directoryObservation(t, ctx, tx, 1, "YVR", now.Add(time.Millisecond))
	page, err = store.ListObserverDirectory(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if page.Coverage.Status != "partial" || page.Coverage.CompleteHours != 0 || page.Coverage.PartialHours != 2 || page.Coverage.MissingHours == 0 || page.EffectiveSort != "traffic" || page.Items[0].ID[15] != 1 || page.MaxObservationCount == nil || *page.MaxObservationCount != 2 || page.Items[0].ObservationCount == nil || *page.Items[0].ObservationCount != 2 {
		t.Fatalf("available raw history and live tail were not counted: %+v", page)
	}
}

func TestObserverDirectoryExactSlicePostgres(t *testing.T) {
	ctx, tx, store := directoryFixture(t)
	at := time.Date(2026, 1, 1, 0, 30, 0, 123000000, time.UTC)
	if _, err := tx.Exec(ctx, `SET LOCAL TIME ZONE 'America/Toronto';
 INSERT INTO analytics_rollup_hours(hour,status) VALUES ('2026-01-01 00:00:00+00','complete');
 INSERT INTO analytics_hourly_observer_identity(hour,iata,observer_id,observation_count)
 VALUES ('2026-01-01 00:00:00+00','YVR','00000000-0000-0000-0000-000000000001',900)`); err != nil {
		t.Fatal(err)
	}
	for _, delta := range []time.Duration{-time.Millisecond, 0, time.Millisecond} {
		directoryObservation(t, ctx, tx, 1, "YVR", at.Add(delta))
	}
	q := api.ObserverDirectoryQuery{IATAs: []string{"YVR"}, Sort: "traffic", Since: at.UnixMilli(), Until: at.Add(time.Millisecond).UnixMilli(), Limit: 10}
	page, err := store.ListObserverDirectory(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if page.WindowStart != q.Since || page.WindowEnd != q.Until || page.MaxObservationCount == nil || *page.MaxObservationCount != 1 || page.Items[0].ObservationCount == nil || *page.Items[0].ObservationCount != 1 || page.Coverage.ExpectedHours != 1 || page.Coverage.PartialHours != 1 || page.Coverage.CompleteHours != 0 {
		t.Fatalf("exact slice included a whole rollup or wrong boundary: %+v", page)
	}
	// A refresh advances the exclusive bound to include the next arrival.
	q.Until++
	page, err = store.ListObserverDirectory(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if page.Items[0].ObservationCount == nil || *page.Items[0].ObservationCount != 2 || page.MaxObservationCount == nil || *page.MaxObservationCount != 2 {
		t.Fatalf("refresh missed new traffic: %+v", page)
	}
}
