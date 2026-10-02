// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestChannelMessageScopesPostgres(t *testing.T) {
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
	for _, table := range []string{"transport_scopes", "packets", "packet_observations", "channels", "channel_messages"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	_, err = tx.Exec(ctx, `
INSERT INTO transport_scopes(id,name,transport_key,key_fingerprint) VALUES
(1,'#yow',decode(repeat('00',16),'hex'),decode(repeat('01',8),'hex')),
(2,'#can',decode(repeat('02',16),'hex'),decode(repeat('03',8),'hex'));
INSERT INTO channels(id,channel_hash,key_fingerprint,name,is_hashtag,key_known) VALUES (123,'\x11','\x01020304','#fixture',true,true);
INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,transport_codes_present,scope_id,raw_payload,raw_header,first_heard_at,last_heard_at)
SELECT decode(lpad(to_hex(i),2,'0'),'hex'),5,0,1,transport,scope_id,'\x00','\x15',NOW(),NOW()
FROM (VALUES(1,true,1),(2,true,NULL),(3,false,NULL),(4,NULL,NULL),(5,true,2),(6,false,NULL)) v(i,transport,scope_id);
INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count)
SELECT packet_hash,md5(n::text)::uuid,'YOW',NOW(),0,1,0 FROM packets CROSS JOIN generate_series(1,2) n;
UPDATE packets SET observation_count=2;
`)
	if err != nil {
		t.Fatal(err)
	}
	s := &Store{q: sqlc.New(tx)}
	// Later matching reception must not relabel the immutable first stored packet.
	scopeID := int32(1)
	_, err = s.UpsertPacket(ctx, ingest.UpsertPacketParams{PacketHash: []byte{6}, PayloadType: 5, ScopeID: &scopeID, TransportCodes: []byte{1, 2, 3, 4}, RawPayload: []byte{0}, RawHeader: []byte{0}})
	if err != nil {
		t.Fatal(err)
	}
	statuses := []api.ChannelScopeStatus{"matched", "unknown", "unscoped", "unavailable", "matched", "unscoped"}
	wants := map[int64]*ingest.InsertedChannelMessage{}
	for n, status := range statuses {
		m := ingest.InsertChannelMessageParams{ChannelID: 123, PacketHash: []byte{byte(n + 1)}, SenderName: "Fixture", Content: "Synthetic message", SentAt: time.Now()}
		inserted, err := s.InsertChannelMessage(ctx, m)
		if err != nil || inserted == nil || inserted.ScopeStatus != status {
			t.Fatalf("insert %d: %+v %v", n, inserted, err)
		}
		wants[inserted.ID] = inserted
		duplicate, err := s.InsertChannelMessage(ctx, m)
		if err != nil || duplicate != nil {
			t.Fatal("duplicate changed live delivery", duplicate, err)
		}
	}
	check := func(items []api.ChannelMessage) {
		t.Helper()
		if len(items) != 6 {
			t.Fatalf("messages=%d", len(items))
		}
		for _, m := range items {
			want := wants[m.ID]
			if want == nil || m.ScopeStatus != want.ScopeStatus {
				t.Fatal("history differs from insertion", m)
			}
			if (m.Scope == nil) != (want.Scope == nil) || (m.Scope != nil && *m.Scope != *want.Scope) {
				t.Fatal("scope differs", m)
			}
			if m.ObservationCount != 2 {
				t.Fatal("duplicate observations changed message grain", m)
			}
			b, _ := json.Marshal(m)
			var wire map[string]any
			_ = json.Unmarshal(b, &wire)
			if _, ok := wire["scope"]; !ok || wire["scopeStatus"] != string(m.ScopeStatus) {
				t.Fatal("wire metadata absent", string(b))
			}
		}
	}
	ch := int32(123)
	for _, id := range []*int32{nil, &ch} {
		p, err := s.ListChannelMessages(ctx, id, time.Time{}, 20, []string{"YOW"}, "", 0)
		if err != nil {
			t.Fatal(err)
		}
		check(p.Items)
	}
	hash, err := s.ListChannelMessagesByHash(ctx, []byte{0x11}, time.Time{}, 20, []string{"YOW"}, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	check(hash.Items)
	catchup, err := s.ListMessagesAfterID(ctx, 0, []string{"YOW"}, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	check(catchup)
	router := chi.NewRouter()
	router.Mount("/channels", handlers.ChannelsRouter(s))
	router.Mount("/messages", handlers.MessagesRouter(s))
	for _, path := range []string{"/channels/123/messages", "/messages", "/messages?channelHash=11", "/messages/backfill?afterId=0"} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest("GET", path, nil).WithContext(ctx))
		if response.Code != 200 {
			t.Fatalf("%s: %d %s", path, response.Code, response.Body.String())
		}
		if path == "/messages/backfill?afterId=0" {
			var items []api.ChannelMessage
			if err := json.Unmarshal(response.Body.Bytes(), &items); err != nil {
				t.Fatal(err)
			}
			check(items)
		} else {
			var body struct {
				Items []api.ChannelMessage `json:"items"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			check(body.Items)
		}
	}
	filtered, err := s.ListChannelMessages(ctx, &ch, time.Time{}, 20, []string{"YOW"}, "#yow", 0)
	if err != nil || len(filtered.Items) != 1 || filtered.Items[0].Scope == nil || *filtered.Items[0].Scope != "#yow" {
		t.Fatal(filtered, err)
	}
	catchup, err = s.ListMessagesAfterID(ctx, 0, []string{"YOW"}, "#yow", 20)
	if err != nil || len(catchup) != 1 {
		t.Fatal(catchup, err)
	}
	other, err := s.ListChannelMessages(ctx, &ch, time.Time{}, 20, []string{"YVR"}, "", 0)
	if err != nil || len(other.Items) != 0 {
		t.Fatal("region escaped", other, err)
	}
	first, err := s.ListChannelMessages(ctx, &ch, time.Time{}, 2, nil, "", 0)
	if err != nil || !first.HasMore || first.NextCursor == nil {
		t.Fatal(first, err)
	}
	second, err := s.ListChannelMessages(ctx, &ch, time.Time{}, 2, nil, "", *first.NextCursor)
	if err != nil || len(second.Items) != 2 || second.Items[0].ID >= *first.NextCursor {
		t.Fatal("cursor drift", second, err)
	}
	// Even without the production cascade, expired packet evidence must not leak orphan messages.
	if _, err := tx.Exec(ctx, "DELETE FROM packets"); err != nil {
		t.Fatal(err)
	}
	expired, err := s.ListChannelMessages(ctx, &ch, time.Time{}, 20, nil, "", 0)
	if err != nil || len(expired.Items) != 0 {
		t.Fatal(expired, err)
	}
}
