// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type endpointQueryCounter struct {
	sqlc.DBTX
	calls int
}

func (q *endpointQueryCounter) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	q.calls++
	return q.DBTX.Query(ctx, sql, args...)
}
func (q *endpointQueryCounter) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	q.calls++
	return q.DBTX.QueryRow(ctx, sql, args...)
}

// Endpoints resolve live against today's nodes, in a fixed number of queries per page.
func TestPacketEndpointsResolveLivePostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for the PostgreSQL regression test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// Own throwaway database so CI can run it against an empty server.
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	name := "beacon_endpoints_" + strings.ToLower(rand.Text())
	ident := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+ident); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP DATABASE "+ident+" WITH (FORCE)")
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec("INSERT INTO iata_codes(iata) VALUES ('YYZ'),('YVR')")

	observer := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	alice, bob, carol := uuid.New(), uuid.New(), uuid.New()
	aliceKey := "aa11" + fmt.Sprintf("%060d", 0)
	observer2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	exec("INSERT INTO observers(id,public_key) VALUES ($1,'\\x01'),($2,'\\x02')", observer, observer2)
	exec(`INSERT INTO nodes(id,public_key,name,node_type) VALUES
 ($1,decode($4,'hex'),'Alice',1), ($2,'\xaa22','Bob',1), ($3,'\xbb33','Carol',2)`, alice, bob, carol, aliceKey)
	// Alice and Bob share prefix aa in YYZ; YVR only knows Alice.
	exec(`INSERT INTO node_short_ids(node_id,iata,prefix_4) VALUES
 ($1,'YYZ','\xaa110000'), ($2,'YYZ','\xaa220000'), ($3,'YYZ','\xbb330000'), ($1,'YVR','\xaa110000')`, alice, bob, carol)

	anchor := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	msg := append([]byte{0xbb, 0xaa, 0, 0}, make([]byte, 16)...)
	packets := []struct {
		kind   int
		raw    []byte
		origin any
		iata   string
	}{
		{2, msg, nil, "YYZ"},                       // 1: direct message, ambiguous source
		{4, []byte{0}, aliceKey, "YYZ"},            // 2: advert from Alice
		{4, []byte{0}, "cc" + aliceKey[2:], "YYZ"}, // 3: advert from an unknown node
		{2, msg, nil, "YVR"},                       // 4: same message bytes, other IATA
	}
	for i, p := range packets {
		n := i + 1
		exec(`INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,origin_pubkey,first_heard_at,last_heard_at)
VALUES (decode($1,'hex'),$2,0,1,$3,'\x00',decode($4,'hex'),$5,$5)`, fmt.Sprintf("%064x", n), p.kind, p.raw, p.origin, anchor.Add(time.Duration(n)*time.Second))
		exec(`INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count)
VALUES ($1,decode($2,'hex'),$3,$4,$5,0,1,0)`, n, fmt.Sprintf("%064x", n), observer, p.iata, anchor.Add(time.Duration(n)*time.Second))
	}
	exec("UPDATE nodes SET name='Alice renamed' WHERE id=$1", alice)

	q := &endpointQueryCounter{DBTX: pool}
	store := &Store{q: sqlc.New(q)}
	check := func(label string, got *api.ResolvedHop, want ...string) {
		t.Helper()
		if names := hopNames(got); !equalStrings(names, want) {
			t.Errorf("%s = %v, want %v", label, names, want)
		}
	}
	checkPage := func(items []api.PacketSummary) {
		t.Helper()
		for _, item := range items {
			lo := item.LatestObserver
			switch item.PacketHash {
			case fmt.Sprintf("%064x", 1):
				check("dm source", lo.ResolvedSource, "ambiguous", "Alice renamed", "Bob")
				check("dm destination", lo.ResolvedDestination, "high", "Carol")
			case fmt.Sprintf("%064x", 2):
				check("advert source", lo.ResolvedSource, "high", "Alice renamed")
			case fmt.Sprintf("%064x", 3):
				check("unknown advert", lo.ResolvedSource, "none")
			case fmt.Sprintf("%064x", 4):
				check("YVR source", lo.ResolvedSource, "high", "Alice renamed")
				check("YVR destination", lo.ResolvedDestination, "none")
			}
		}
	}

	// One page query plus one hash batch and one advert batch.
	for _, iatas := range [][]string{nil, {"YYZ", "YVR"}} {
		q.calls = 0
		page, err := store.ListPackets(ctx, nil, nil, iatas, nil, time.Time{}, time.Time{}, 0, 50)
		if err != nil || len(page.Items) != 4 || q.calls != 3 {
			t.Fatalf("list %v: rows=%d queries=%d err=%v", iatas, len(page.Items), q.calls, err)
		}
		checkPage(page.Items)
	}
	q.calls = 0
	backfill, err := store.ListPacketsAfterID(ctx, 0, -1, -1, nil, "", 50)
	if err != nil || len(backfill) != 4 || q.calls != 3 {
		t.Fatalf("backfill: rows=%d queries=%d err=%v", len(backfill), q.calls, err)
	}
	checkPage(backfill)

	// Detail resolves every observation in one batch, per its own IATA.
	exec(`INSERT INTO packet_observations(id,packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count)
VALUES (5,decode($1,'hex'),$2,'YVR',$3,0,1,0)`, fmt.Sprintf("%064x", 1), observer2, anchor.Add(10*time.Second))
	q.calls = 0
	packet, err := store.GetPacket(ctx, append(make([]byte, 31), 1))
	if err != nil || len(packet.Observations) != 2 {
		t.Fatalf("detail: %v", err)
	}
	if q.calls != 3 {
		t.Errorf("detail used %d queries, want packet, observations and one endpoint batch", q.calls)
	}
	check("YYZ observation", packet.Observations[0].ResolvedSource, "ambiguous", "Alice renamed", "Bob")
	check("YVR observation", packet.Observations[1].ResolvedSource, "high", "Alice renamed")
}
