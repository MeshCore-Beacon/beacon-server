// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"strings"
	"testing"
)

// cmiMigrations are the files that build channel_message_iatas, its indexes and the channel walk index.
var cmiMigrations = []string{
	"035_channel_message_iatas.sql",
	"036_cmi_channel_site.sql",
	"037_cmi_site.sql",
	"038_cmi_channel_site_scope.sql",
	"039_cmi_channel_scope.sql",
	"040_cmi_scope.sql",
	"041_channel_messages_channel_id.sql",
}

// The migrations leave channel_message_iatas and channel_messages with exactly these indexes, all usable, and a
// message's site rows go with it.
func TestCmiMigrationsIndexSetPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	checkRecorded(t, ctx, pool, cmiMigrations...)
	var schema string
	if err := pool.QueryRow(ctx, "SELECT current_schema()").Scan(&schema); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"channel_message_iatas_pkey":       "CREATE UNIQUE INDEX channel_message_iatas_pkey ON public.channel_message_iatas USING btree (message_id, iata)",
		"idx_cmi_channel_site":             "CREATE INDEX idx_cmi_channel_site ON public.channel_message_iatas USING btree (channel_id, iata, message_id DESC)",
		"idx_cmi_site":                     "CREATE INDEX idx_cmi_site ON public.channel_message_iatas USING btree (iata, message_id DESC)",
		"idx_cmi_channel_site_scope":       "CREATE INDEX idx_cmi_channel_site_scope ON public.channel_message_iatas USING btree (channel_id, iata, scope_id, message_id DESC) WHERE (scope_id IS NOT NULL)",
		"idx_cmi_channel_scope":            "CREATE INDEX idx_cmi_channel_scope ON public.channel_message_iatas USING btree (channel_id, scope_id, message_id DESC) WHERE (scope_id IS NOT NULL)",
		"idx_cmi_scope":                    "CREATE INDEX idx_cmi_scope ON public.channel_message_iatas USING btree (scope_id, message_id DESC) WHERE (scope_id IS NOT NULL)",
		"channel_messages_pkey":            "CREATE UNIQUE INDEX channel_messages_pkey ON public.channel_messages USING btree (id)",
		"channel_messages_packet_hash_key": "CREATE UNIQUE INDEX channel_messages_packet_hash_key ON public.channel_messages USING btree (packet_hash)",
		"idx_channel_messages_channel_id":  "CREATE INDEX idx_channel_messages_channel_id ON public.channel_messages USING btree (channel_id, id DESC)",
		// Read only by the fallback statements; dropped a release later.
		"idx_channel_messages_channel":   "CREATE INDEX idx_channel_messages_channel ON public.channel_messages USING btree (channel_id, sent_at DESC)",
		"idx_channel_messages_sent_brin": "CREATE INDEX idx_channel_messages_sent_brin ON public.channel_messages USING brin (sent_at)",
	}
	rows, err := pool.Query(ctx, `SELECT i.indexname, i.indexdef, x.indisvalid AND x.indisready
FROM pg_indexes i JOIN pg_index x ON x.indexrelid = format('%I.%I', i.schemaname, i.indexname)::regclass
WHERE i.schemaname = current_schema() AND i.tablename IN ('channel_message_iatas', 'channel_messages')`)
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

	var cols []string
	if err := pool.QueryRow(ctx, `SELECT array_agg(column_name || ' ' || data_type || CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE ' NULL' END ORDER BY ordinal_position)
FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'channel_message_iatas'`).Scan(&cols); err != nil {
		t.Fatal(err)
	}
	wantCols := "message_id bigint NOT NULL,iata character NOT NULL,channel_id integer NOT NULL,scope_id integer NULL"
	if strings.Join(cols, ",") != wantCols {
		t.Errorf("columns = %v, want %s", cols, wantCols)
	}
	var iataLen int
	if err := pool.QueryRow(ctx, `SELECT character_maximum_length FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = 'channel_message_iatas' AND column_name = 'iata'`).Scan(&iataLen); err != nil || iataLen != 3 {
		t.Errorf("iata length = %d, %v; want character(3)", iataLen, err)
	}

	// A packet's deletion cascades through its message to the message's site rows.
	if _, err := pool.Exec(ctx, `
INSERT INTO channels (id, channel_hash, name) VALUES (7, '\x11', 'fixture');
INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, raw_payload, raw_header, first_heard_at, last_heard_at)
VALUES ('\x01', 5, 0, 1, '\x00', '\x15', now(), now());
INSERT INTO channel_messages (id, channel_id, packet_hash, sent_at) VALUES (3000000001, 7, '\x01', now());
INSERT INTO channel_message_iatas (message_id, iata, channel_id) VALUES (3000000001, 'YUL', 7);
DELETE FROM packets;`); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM channel_message_iatas").Scan(&left); err != nil || left != 0 {
		t.Fatalf("cmi rows after the packet delete = %d, %v", left, err)
	}
}
