// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/MeshCore-Beacon/beacon-server/internal/cache"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-chi/chi/v5"
)

func TestEmptyRegionListFiltersPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	// Guard the measurement columns used by region-filtered readers. MeshMapper's
	// provider catalogue has unrelated text IATAs and does not use this marker.
	wantColumns := map[string]bool{}
	for _, table := range []string{
		"iata_codes", "channel_iatas", "known_routes", "node_iatas", "node_neighbors", "node_short_ids",
		"observer_locations", "packet_observations", "region_iatas", "trace_iatas", "packet_sites", "channel_message_iatas",
		"analytics_hourly_iata_observations", "analytics_hourly_payload_breakdown", "analytics_hourly_signal",
		"analytics_hourly_paths", "analytics_hourly_observer_identity", "analytics_hourly_advert_hearings",
		"analytics_hourly_scope_observers", "analytics_hourly_scope_nodes",
	} {
		wantColumns[table+".iata"] = false
	}
	wantColumns["observers.last_iata"] = false
	for _, table := range []string{"analytics_hourly_advert_sets", "analytics_hourly_talker_sets", "analytics_hourly_packet_sets", "analytics_hourly_scope_sets"} {
		wantColumns[table+".iatas"] = false
	}
	columns, err := pool.Query(ctx, `SELECT c.relname, a.attname, format_type(a.atttypid,a.atttypmod)
FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname=current_schema() AND c.relkind IN ('r','p') AND NOT a.attisdropped
AND a.attname IN ('iata','last_iata','iatas')`)
	if err != nil {
		t.Fatal(err)
	}
	defer columns.Close()
	for columns.Next() {
		var table, name, kind string
		if err := columns.Scan(&table, &name, &kind); err != nil {
			t.Fatal(err)
		}
		column := table + "." + name
		if _, required := wantColumns[column]; !required {
			continue
		}
		want := "character(3)"
		if name == "iatas" {
			want += "[]"
		}
		if kind != want {
			t.Fatalf("empty-region marker contract changed: %s.%s is %s, want %s", table, name, kind, want)
		}
		wantColumns[column] = true
	}
	if err := columns.Err(); err != nil {
		t.Fatal(err)
	}
	columns.Close()
	for column, seen := range wantColumns {
		if !seen {
			t.Fatalf("IATA width contract was not checked for %s", column)
		}
	}
	// Include blank, NULL and the marker's three-character prefix as legacy data.
	// A reader that drops or truncates the marker will therefore leak real rows.
	_, err = pool.Exec(ctx, `
INSERT INTO iata_codes(iata) VALUES (''),('---'),('YKF');
INSERT INTO regions(id,slug,name) VALUES (1,'empty','Empty fixture');
INSERT INTO observers(id,public_key,display_name,last_iata)
 SELECT lpad(i::text,32,'0')::uuid,decode(repeat(lpad(to_hex(i),2,'0'),32),'hex'),'Observer '||i,iata
 FROM (VALUES (1,''),(2,'---'),(3,'YKF'),(4,NULL)) v(i,iata);
INSERT INTO nodes(id,public_key,node_type,name)
 SELECT lpad((i+10)::text,32,'0')::uuid,decode(repeat(lpad(to_hex(i+10),2,'0'),32),'hex'),2,'Node '||i
 FROM generate_series(1,3) i;
INSERT INTO node_iatas(node_id,iata)
 SELECT lpad((i+10)::text,32,'0')::uuid,iata FROM (VALUES (1,''),(2,'---'),(3,'YKF')) v(i,iata);
INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,parsed_payload,observation_count,first_heard_at,last_heard_at)
 SELECT decode(lpad(i::text,16,'0'),'hex'),5,0,1,decode('','hex'),decode('01','hex'),'{}',1,now(),now() FROM generate_series(1,3) i;
INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,path_bytes,payload_type)
 SELECT i,decode(lpad(i::text,16,'0'),'hex'),lpad(i::text,32,'0')::uuid,iata,now(),0,1,0,decode('','hex'),5
 FROM (VALUES (1,''),(2,'---'),(3,'YKF')) v(i,iata);
INSERT INTO packet_sites(packet_hash,iata,site_heard_at,first_heard_at,payload_type,route_type)
 SELECT decode(lpad(i::text,16,'0'),'hex'),iata,now(),now(),5,1 FROM (VALUES (1,''),(2,'---'),(3,'YKF')) v(i,iata);
INSERT INTO channels(id,channel_hash,name) VALUES (1,decode('ab','hex'),'Fixture');
INSERT INTO channel_messages(id,channel_id,packet_hash,sender_name,content,sent_at)
 SELECT i,1,decode(lpad(i::text,16,'0'),'hex'),'Fixture','Fixture message',now() FROM generate_series(1,3) i;
INSERT INTO trace_tags(trace_tag,first_heard_at,last_heard_at,packet_count,trace_type)
 SELECT decode(lpad(i::text,8,'0'),'hex'),now(),now(),1,'TRACE' FROM generate_series(1,3) i;
INSERT INTO trace_iatas(trace_tag,iata)
 SELECT decode(lpad(i::text,8,'0'),'hex'),iata FROM (VALUES (1,''),(2,'---'),(3,'YKF')) v(i,iata);
INSERT INTO repeater_readings(target_public_key,observer_public_key,observer_id,sampled_at,received_at,method,tag,battery_mv,trusted)
 SELECT decode(repeat(lpad(to_hex(i+10),2,'0'),32),'hex'),decode(repeat(lpad(to_hex(i),2,'0'),32),'hex'),
 lpad(i::text,32,'0')::uuid,now(),now(),'status',i,4000,true FROM generate_series(1,3) i;
`)
	if err != nil {
		t.Fatal(err)
	}
	store := New(pool, pool, 0, 0, WithReadModels(earnedPacketSites()))
	routerFor := func(reader api.Reader) http.Handler {
		r := chi.NewRouter()
		r.Mount("/nodes", handlers.NodesRouter(reader, true))
		r.Mount("/observers", handlers.ObserversRouter(reader))
		r.Mount("/packets", handlers.PacketsRouter(reader))
		r.Mount("/messages", handlers.MessagesRouter(reader))
		r.Mount("/traces", handlers.TracesRouter(reader))
		r.Mount("/stats", handlers.StatsRouter(reader, handlers.StatsOptions{}))
		return r
	}
	read := func(t *testing.T, router http.Handler, path, query string) int {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, path, nil)
		if query != "" {
			if r.URL.RawQuery != "" {
				r.URL.RawQuery += "&"
			}
			r.URL.RawQuery += query
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatalf("%s %s: HTTP %d: %s", path, query, w.Code, w.Body.String())
		}
		var rows []json.RawMessage
		if strings.HasPrefix(w.Body.String(), "[") {
			if err := json.Unmarshal(w.Body.Bytes(), &rows); err != nil {
				t.Fatal(err)
			}
		} else {
			var page struct {
				Items       []json.RawMessage
				HasMore     bool
				Sort, Order string
				GeneratedAt int64
			}
			if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
				t.Fatal(err)
			}
			rows = page.Items
			if page.HasMore {
				t.Fatal("fixture returned unexpected continuation")
			}
			if path == "/nodes" && (page.Sort != "lastSeen" || page.Order != "desc") {
				t.Fatal("node sort metadata changed")
			}
			if path == "/nodes?sort=name" && (page.Sort != "name" || page.Order != "asc") {
				t.Fatal("sorted node metadata changed")
			}
			if path == "/nodes/telemetry/latest" && page.GeneratedAt == 0 {
				t.Fatal("latest readings lost generatedAt")
			}
		}
		if rows == nil {
			t.Fatal("response rows must be [] rather than null")
		}
		return len(rows)
	}
	router := routerFor(store)
	for _, path := range []string{"/nodes", "/nodes?sort=name", "/nodes/telemetry/latest", "/observers", "/packets", "/packets?hashPrefix=0000", "/packets/backfill?afterObservationId=0", "/messages/backfill?afterId=0", "/traces", "/stats/node-types"} {
		t.Run(path, func(t *testing.T) {
			if read(t, router, path, "") == 0 {
				t.Fatal("fixture has no global rows")
			}
			if read(t, router, path, "iata=%20") != 1 {
				t.Fatal("blank legacy IATA fixture is missing")
			}
			if read(t, router, path, "iata=---") != 1 {
				t.Fatal("three-character prefix fixture is missing")
			}
			for _, q := range []string{"region=empty", "regionId=1"} {
				if read(t, router, path, q) != 0 {
					t.Fatalf("%s returned global/legacy rows", q)
				}
			}
			if read(t, router, path, "region=empty&iata=YKF") != 1 {
				t.Fatal("explicit IATA union was lost")
			}
		})
	}
	// Preserve the normal packet read-model readiness error, even for empty regions.
	w := httptest.NewRecorder()
	routerFor(New(pool, pool, 0, 0)).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/packets?region=empty", nil))
	if w.Code != 503 || w.Header().Get("Retry-After") == "" {
		t.Fatalf("packet readiness changed: %d %s", w.Code, w.Body.String())
	}
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "cache global first", true: "cache empty first"}[reverse], func(t *testing.T) {
			redis := miniredis.RunT(t)
			c := cache.NewClient(redis.Addr(), "", 0)
			t.Cleanup(func() { c.Close() })
			cached := routerFor(cache.NewCachedReader(store, c, cache.CacheTTLs{Reference: time.Minute, Stats: time.Minute}))
			queries := []string{"", "region=empty"}
			if reverse {
				queries[0], queries[1] = queries[1], queries[0]
			}
			for range 2 {
				for _, q := range queries {
					want := 3
					if q != "" {
						want = 0
					}
					if got := read(t, cached, "/nodes/telemetry/latest", q); got != want {
						t.Fatalf("cached %q returned %d rows, want %d", q, got, want)
					}
				}
			}
			// Stats' single-region/cache path must also distinguish no selection from an empty one.
			for range 2 {
				for _, q := range queries {
					want := 1 // the fixture contains one node type
					if q != "" {
						want = 0
					}
					if got := read(t, cached, "/stats/node-types", q); got != want {
						t.Fatalf("cached stats %q returned %d rows, want %d", q, got, want)
					}
				}
			}
		})
	}
}
