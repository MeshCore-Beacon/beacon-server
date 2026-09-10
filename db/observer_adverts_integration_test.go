// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/jackc/pgx/v5"
)

func TestObserverAdvertsPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
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
	_, err = tx.Exec(ctx, `
CREATE TEMP TABLE packets (LIKE public.packets INCLUDING ALL) ON COMMIT DROP;
CREATE TEMP TABLE packet_observations (LIKE public.packet_observations INCLUDING ALL) ON COMMIT DROP;
CREATE TEMP TABLE nodes (LIKE public.nodes INCLUDING ALL) ON COMMIT DROP;
INSERT INTO nodes(public_key,node_type,name) VALUES (decode(repeat('01',32),'hex'),1,'Known fixture node');
INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,origin_pubkey,raw_payload,raw_header,first_heard_at,last_heard_at) VALUES
 (decode(repeat('a1',32),'hex'),4,0,1,NULL,'\x00','\x00',now(),now()),
 (decode(repeat('b2',32),'hex'),4,0,1,decode(repeat('01',32),'hex'),'\x00','\x00',now(),now());
INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count) VALUES
 (1,decode(repeat('a1',32),'hex'),'00000000-0000-0000-0000-000000000106','YOW',now(),0,1,0),
 (2,decode(repeat('b2',32),'hex'),'00000000-0000-0000-0000-000000000106','YOW',now(),0,1,0);`)
	if err != nil {
		t.Fatal(err)
	}
	router := handlers.ObserversRouter(&Store{q: sqlc.New(tx)})
	request := func(query string) api.Page[api.AdvertObservation] {
		t.Helper()
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/00000000-0000-0000-0000-000000000106/adverts?"+query, nil).WithContext(ctx))
		if response.Code != http.StatusOK {
			t.Fatalf("advert page HTTP %d", response.Code)
		}
		var page api.Page[api.AdvertObservation]
		if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		return page
	}
	page := request("limit=50")
	if len(page.Items) != 2 || page.Items[0].ID != 1 || page.Items[1].ID != 2 {
		t.Fatal("missing-origin advert was not preserved in the ordered page")
	}
	if key := page.Items[0].NodePublicKey; key == nil || *key != "" {
		t.Errorf("missing origin key = %v, want an empty string", key)
	}
	if key := page.Items[1].NodePublicKey; key == nil || *key != strings.Repeat("01", 32) {
		t.Error("known origin key changed")
	}
	first := request("limit=1")
	if !first.HasMore || first.NextCursor == nil || *first.NextCursor != 1 {
		t.Fatal("missing-origin advert broke the cursor")
	}
	last := request("limit=1&cursor=1")
	if len(last.Items) != 1 || last.Items[0].ID != 2 || last.HasMore {
		t.Fatal("continuing after the missing-origin advert lost the known advert")
	}
}
