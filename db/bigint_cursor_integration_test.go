// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"testing"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// The four statements before the ::bigint casts, verbatim from origin/dev.
const (
	legacyListObserverAdverts = `SELECT 
  po.id,
  encode(po.packet_hash, 'hex') AS packet_hash_hex,
  p.payload_type,
  po.iata,
  po.heard_at,
  po.rssi,
  po.snr,
  po.hop_count,
  n.name AS node_name,
  COALESCE(encode(p.origin_pubkey, 'hex'), '')::text AS node_public_key
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
LEFT JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE po.observer_id = $1
  AND p.payload_type = 4
  AND ($2 = 0 OR po.id > $2)
ORDER BY po.id ASC
LIMIT $3`
	legacyListNodeObservations = `SELECT po.id, encode(po.packet_hash, 'hex') AS packet_hash_hex,
  p.payload_type, po.iata, po.heard_at, po.rssi, po.snr, po.hop_count
FROM packet_observations po
JOIN packets p ON p.packet_hash = po.packet_hash
JOIN nodes n ON n.public_key = p.origin_pubkey
WHERE n.id = $1
  AND ($2 = 0 OR po.id < $2)
ORDER BY po.id DESC
LIMIT $3`
	legacyGetObserverTelemetry = `SELECT id, reported_at, battery_voltage_mv, airtime_tx_secs, airtime_rx_secs,
       noise_floor_db, uptime_seconds, queue_length, debug_flags, receive_errors
FROM observer_telemetry
WHERE observer_id = $1
  AND ($2::timestamptz IS NULL OR reported_at >= $2)
  AND ($3::timestamptz IS NULL OR reported_at <= $3)
  AND ($4 = 0 OR id > $4)
ORDER BY reported_at ASC`
	legacyListAllChannelMessages = `SELECT DISTINCT ON (cm.id) cm.*, encode(cm.packet_hash, 'hex') as packet_hash_hex, c.channel_hash, ts.name AS scope_name, p.transport_codes_present,
p.observation_count AS observation_count
FROM channel_messages cm
JOIN channels c ON c.id = cm.channel_id
JOIN packet_observations po ON po.packet_hash = cm.packet_hash
JOIN packets p ON p.packet_hash = cm.packet_hash
LEFT JOIN transport_scopes ts ON ts.id = p.scope_id
WHERE ($1::timestamptz IS NULL OR cm.sent_at >= $1)
  AND (COALESCE(cardinality($2::bpchar[]), 0) = 0 OR po.iata = ANY($2::bpchar[]))
  AND ($3::text = '' OR ts.name = $3::text)
  AND ($4 = 0 OR cm.id < $4)
ORDER BY cm.id DESC
LIMIT $5`
)

const (
	cursorObserver = "00000000-0000-0000-0000-000000000106"
	cursorNode     = "00000000-0000-0000-0000-000000000201"
)

// Ids 1-3 sit below 2^31; each sequence then hands out 2147483646..2147483651, crossing it as prod's will.
const bigintCursorFixture = `
INSERT INTO observers(id,public_key) VALUES ('00000000-0000-0000-0000-000000000106','\x06');
INSERT INTO nodes(id,public_key,node_type,name) VALUES ('00000000-0000-0000-0000-000000000201','\x01',1,'Fixture');
INSERT INTO channels(id,channel_hash,name) VALUES (7,'\x11','#fixture');
INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,origin_pubkey,raw_payload,raw_header,first_heard_at,last_heard_at)
SELECT int4send(i),CASE WHEN i % 2 = 0 THEN 4 ELSE 5 END,0,1,'\x01','\x00','\x00',now(),now() FROM generate_series(1,9) i;
INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count)
SELECT i,int4send(i),'00000000-0000-0000-0000-000000000106','YOW',now(),0,1,0 FROM generate_series(1,3) i;
SELECT setval('packet_observations_id_seq', 2147483645);
INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count)
SELECT int4send(i),'00000000-0000-0000-0000-000000000106','YOW',now(),0,1,0 FROM generate_series(4,9) i;
INSERT INTO channel_messages(id,channel_id,packet_hash,sent_at) SELECT i,7,int4send(i),now() FROM generate_series(1,3) i;
SELECT setval('channel_messages_id_seq', 2147483645);
INSERT INTO channel_messages(channel_id,packet_hash,sent_at) SELECT 7,int4send(i),now() FROM generate_series(4,9) i;
INSERT INTO observer_telemetry(id,observer_id,reported_at)
SELECT i,'00000000-0000-0000-0000-000000000106',date_trunc('minute',now())-make_interval(mins=>60-i) FROM generate_series(1,3) i;
SELECT setval('observer_telemetry_id_seq', 2147483645);
INSERT INTO observer_telemetry(observer_id,reported_at)
SELECT '00000000-0000-0000-0000-000000000106',date_trunc('minute',now())-make_interval(mins=>60-i) FROM generate_series(4,9) i;`

func getJSON(t *testing.T, ctx context.Context, h http.Handler, path string, out any) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s: HTTP %d %s", path, rec.Code, rec.Body)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
		t.Fatal(err)
	}
}

// walkPages follows nextCursor at limit=1, so every id, those past 2^31 included, goes back as a cursor.
// base ends in "?" or "&".
func walkPages[T any](t *testing.T, ctx context.Context, h http.Handler, base string, id func(T) int64) []int64 {
	t.Helper()
	var ids []int64
	path := base + "limit=1"
	for range 20 {
		var page api.Page[T]
		getJSON(t, ctx, h, path, &page)
		for _, item := range page.Items {
			ids = append(ids, id(item))
		}
		if !page.HasMore {
			return ids
		}
		path = fmt.Sprintf("%slimit=1&cursor=%d", base, *page.NextCursor)
	}
	t.Fatalf("%s: still paging after 20 pages", base)
	return nil
}

func queryInt64s(t *testing.T, ctx context.Context, tx pgx.Tx, sql string, args ...any) []int64 {
	t.Helper()
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func legacyRows[T any](t *testing.T, ctx context.Context, tx pgx.Tx, sql string, args ...any) []T {
	t.Helper()
	rows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[T])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func sameRows[T any](got, want []T) bool {
	return len(got) == len(want) && (len(got) == 0 || reflect.DeepEqual(got, want))
}

// Observation ids pass 2^31 in 2026-11; every id cursor must keep paging past it.
func TestBigintCursorsPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	if _, err := tx.Exec(ctx, bigintCursorFixture); err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	observers, nodes, messages, packets := handlers.ObserversRouter(store), handlers.NodesRouter(store), handlers.MessagesRouter(store), handlers.PacketsRouter(store)

	t.Run("observer adverts", func(t *testing.T) {
		want := queryInt64s(t, ctx, tx, `SELECT po.id FROM packet_observations po JOIN packets p USING (packet_hash) WHERE p.payload_type = 4 ORDER BY po.id`)
		got := walkPages(t, ctx, observers, "/"+cursorObserver+"/adverts?", func(a api.AdvertObservation) int64 { return a.ID })
		if !slices.Equal(got, want) || want[len(want)-1] <= math.MaxInt32+1 {
			t.Fatalf("got %v, want %v", got, want)
		}
		var past api.Page[api.AdvertObservation]
		getJSON(t, ctx, observers, fmt.Sprintf("/%s/adverts?cursor=%d", cursorObserver, int64(math.MaxInt64)), &past)
		if len(past.Items) != 0 || past.HasMore {
			t.Fatalf("cursor MaxInt64: %+v", past)
		}
	})
	t.Run("node observations", func(t *testing.T) {
		want := queryInt64s(t, ctx, tx, `SELECT id FROM packet_observations ORDER BY id DESC`)
		got := walkPages(t, ctx, nodes, "/"+cursorNode+"/observations?", func(o api.PacketObservationSummary) int64 { return o.ID })
		if !slices.Equal(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
	t.Run("messages", func(t *testing.T) {
		want := queryInt64s(t, ctx, tx, `SELECT id FROM channel_messages ORDER BY id DESC`)
		for name, base := range map[string]string{"all channels": "/?", "channel id": "/?channelID=7&", "channel hash": "/?channelHash=11&"} {
			t.Run(name, func(t *testing.T) {
				if got := walkPages(t, ctx, messages, base, func(m api.ChannelMessage) int64 { return m.ID }); !slices.Equal(got, want) {
					t.Fatalf("got %v, want %v", got, want)
				}
			})
		}
	})
	t.Run("telemetry", func(t *testing.T) {
		for _, after := range []int64{0, 2, math.MaxInt32, math.MaxInt32 + 1, math.MaxInt32 + 3} {
			want := queryInt64s(t, ctx, tx, `SELECT (extract(epoch FROM reported_at) * 1000)::bigint FROM observer_telemetry WHERE id > $1 ORDER BY reported_at`, after)
			var tel api.ObserverTelemetry
			getJSON(t, ctx, observers, fmt.Sprintf("/%s/telemetry?afterId=%d", cursorObserver, after), &tel)
			got := make([]int64, 0, len(tel.Points))
			for _, p := range tel.Points {
				got = append(got, p.T)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("afterId %d: got %v, want %v", after, got, want)
			}
		}
	})
	t.Run("ws backfill", func(t *testing.T) {
		var ps []api.PacketSummary
		getJSON(t, ctx, packets, fmt.Sprintf("/backfill?afterObservationId=%d", math.MaxInt32), &ps)
		var ms []api.ChannelMessage
		getJSON(t, ctx, messages, fmt.Sprintf("/backfill?afterId=%d", math.MaxInt32), &ms)
		if len(ps) != 4 || len(ms) != 4 {
			t.Fatalf("backfill past 2^31: %d packets, %d messages, want 4 and 4", len(ps), len(ms))
		}
	})
	t.Run("same rows as today below 2^31", func(t *testing.T) {
		q := sqlc.New(tx)
		obs, node := uuid.MustParse(cursorObserver), uuid.MustParse(cursorNode)
		for _, c := range []int32{-1, 0, 1, 2, 3, 2147483646, math.MaxInt32} {
			for _, n := range []int32{1, 2, 50} {
				adverts, err := q.ListObserverAdverts(ctx, sqlc.ListObserverAdvertsParams{ObserverID: obs, Column2: int64(c), Limit: n})
				if err != nil || !sameRows(adverts, legacyRows[sqlc.ListObserverAdvertsRow](t, ctx, tx, legacyListObserverAdverts, obs, c, n)) {
					t.Fatalf("adverts cursor %d limit %d differ: %v", c, n, err)
				}
				nodeObs, err := q.ListNodeObservations(ctx, sqlc.ListNodeObservationsParams{ID: node, Column2: int64(c), Limit: n})
				if err != nil || !sameRows(nodeObs, legacyRows[sqlc.ListNodeObservationsRow](t, ctx, tx, legacyListNodeObservations, node, c, n)) {
					t.Fatalf("node observations cursor %d limit %d differ: %v", c, n, err)
				}
				msgs, err := q.ListAllChannelMessages(ctx, sqlc.ListAllChannelMessagesParams{Column4: int64(c), Limit: n})
				if err != nil || !sameRows(msgs, legacyRows[sqlc.ListAllChannelMessagesRow](t, ctx, tx, legacyListAllChannelMessages, pgtype.Timestamptz{}, []string(nil), "", c, n)) {
					t.Fatalf("messages cursor %d limit %d differ: %v", c, n, err)
				}
			}
			tel, err := q.GetObserverTelemetry(ctx, sqlc.GetObserverTelemetryParams{ObserverID: obs, Column4: int64(c)})
			if err != nil || !sameRows(tel, legacyRows[sqlc.GetObserverTelemetryRow](t, ctx, tx, legacyGetObserverTelemetry, obs, pgtype.Timestamptz{}, pgtype.Timestamptz{}, c)) {
				t.Fatalf("telemetry cursor %d differ: %v", c, err)
			}
		}
	})
}
