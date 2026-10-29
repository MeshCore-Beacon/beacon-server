// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// packetSitesMigrations are the files that build packet_sites and its three secondary indexes.
var packetSitesMigrations = []string{
	"042_packet_sites.sql",
	"043_packet_sites_site.sql",
	"044_packet_sites_site_type.sql",
	"045_packet_sites_site_scope.sql",
	"054_packet_sites_late.sql",
	"055_packet_sites_early.sql",
	"059_packet_sites_site_scope_type.sql",
}

// The migrations leave packet_sites with exactly these indexes, all usable, its column types, statistics target
// and autovacuum settings; the type domains hold and a packet's site rows go with it.
func TestPacketSitesMigrationsIndexSetPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	checkRecorded(t, ctx, pool, packetSitesMigrations...)
	var schema string
	if err := pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"packet_sites_pkey":                "CREATE UNIQUE INDEX packet_sites_pkey ON public.packet_sites USING btree (packet_hash, iata) INCLUDE (site_heard_at)",
		"idx_packet_sites_site":            "CREATE INDEX idx_packet_sites_site ON public.packet_sites USING btree (iata, site_heard_at DESC, packet_hash DESC) INCLUDE (payload_type, route_type, first_heard_at)",
		"idx_packet_sites_site_type":       "CREATE INDEX idx_packet_sites_site_type ON public.packet_sites USING btree (iata, payload_type, route_type, site_heard_at DESC, packet_hash DESC)",
		"idx_packet_sites_site_scope":      "CREATE INDEX idx_packet_sites_site_scope ON public.packet_sites USING btree (iata, scope_id, site_heard_at DESC, packet_hash DESC) INCLUDE (payload_type, route_type, first_heard_at) WHERE (scope_id IS NOT NULL)",
		"idx_packet_sites_late":            "CREATE INDEX idx_packet_sites_late ON public.packet_sites USING btree (iata, site_heard_at DESC, packet_hash DESC) INCLUDE (first_heard_at, payload_type, route_type, scope_id) WHERE ((site_heard_at - first_heard_at) > '00:30:00'::interval)",
		"idx_packet_sites_early":           "CREATE INDEX idx_packet_sites_early ON public.packet_sites USING btree (iata, first_heard_at) INCLUDE (site_heard_at, packet_hash, payload_type, route_type, scope_id) WHERE ((first_heard_at - site_heard_at) > '00:30:00'::interval)",
		"idx_packet_sites_site_scope_type": "CREATE INDEX idx_packet_sites_site_scope_type ON public.packet_sites USING btree (iata, scope_id, payload_type, route_type, site_heard_at DESC, packet_hash DESC) INCLUDE (first_heard_at) WHERE (scope_id IS NOT NULL)",
	}
	rows, err := pool.Query(ctx, `SELECT i.indexname, i.indexdef, x.indisvalid AND x.indisready
FROM pg_indexes i JOIN pg_index x ON x.indexrelid = format('%I.%I', i.schemaname, i.indexname)::regclass
WHERE i.schemaname = current_schema() AND i.tablename = 'packet_sites'`)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for rows.Next() {
		var name, def string
		var ok bool
		if err := rows.Scan(&name, &def, &ok); err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Errorf("%s is not valid and ready", name)
		}
		got[name] = strings.Replace(def, " ON "+schema+".", " ON public.", 1)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for name, def := range want {
		if got[name] != def {
			t.Errorf("%s:\n got %q\nwant %q", name, got[name], def)
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("unexpected index %s: %s", name, got[name])
		}
	}

	var stat int
	if err := pool.QueryRow(ctx, `SELECT attstattarget FROM pg_attribute
WHERE attrelid = 'packet_sites'::regclass AND attname = 'iata'`).Scan(&stat); err != nil || stat != 500 {
		t.Errorf("iata statistics target = %d, %v; want 500", stat, err)
	}
	var opts []string
	if err := pool.QueryRow(ctx, "SELECT reloptions FROM pg_class WHERE oid = 'packet_sites'::regclass").Scan(&opts); err != nil {
		t.Fatal(err)
	}
	if strings.Join(opts, ",") != "autovacuum_vacuum_scale_factor=0.05,autovacuum_vacuum_cost_limit=1000" {
		t.Errorf("reloptions = %v", opts)
	}

	var cols []string
	if err := pool.QueryRow(ctx, `SELECT array_agg(column_name || ' ' || data_type || CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE ' NULL' END ORDER BY ordinal_position)
FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'packet_sites'`).Scan(&cols); err != nil {
		t.Fatal(err)
	}
	wantCols := "packet_hash bytea NOT NULL,iata character NOT NULL,site_heard_at timestamp with time zone NOT NULL," +
		"first_heard_at timestamp with time zone NOT NULL,payload_type smallint NOT NULL,route_type smallint NOT NULL,scope_id integer NULL"
	if strings.Join(cols, ",") != wantCols {
		t.Errorf("columns = %v, want %s", cols, wantCols)
	}
	var iataLen int
	if err := pool.QueryRow(ctx, `SELECT character_maximum_length FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = 'packet_sites' AND column_name = 'iata'`).Scan(&iataLen); err != nil || iataLen != 3 {
		t.Errorf("iata length = %d, %v; want character(3)", iataLen, err)
	}

	if _, err := pool.Exec(ctx, `
INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, raw_payload, raw_header, first_heard_at, last_heard_at)
VALUES ('\x01', 4, 0, 1, '\x00', '\x11', now(), now());`); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		`INSERT INTO packet_sites VALUES ('\x01', 'AAA', now(), now(), 16, 1, NULL)`,
		`INSERT INTO packet_sites VALUES ('\x01', 'AAA', now(), now(), 4, 4, NULL)`,
	} {
		_, err := pool.Exec(ctx, bad)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
			t.Errorf("%s: got %v, want 23514", bad, err)
		}
	}

	// A packet's deletion cascades to its observations and its site rows.
	if _, err := pool.Exec(ctx, `
INSERT INTO iata_codes (iata) VALUES ('AAA') ON CONFLICT DO NOTHING;
INSERT INTO observers (id, public_key) VALUES ('00000000-0000-0000-0000-000000000001', '\x01');
INSERT INTO packet_observations (packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count)
VALUES ('\x01', '00000000-0000-0000-0000-000000000001', 'AAA', now(), 0, 1, 0);
INSERT INTO packet_sites VALUES ('\x01', 'AAA', now(), now(), 4, 1, NULL);
DELETE FROM packets;`); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM packet_sites").Scan(&left); err != nil || left != 0 {
		t.Fatalf("packet_sites rows after the packet delete = %d, %v", left, err)
	}
}
