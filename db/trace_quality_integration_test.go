package db

import (
	"context"
	"encoding/hex"
	"os"
	"testing"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
)

func TestTraceQualityPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	for _, name := range []string{"nodes", "node_short_ids"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+name+" (LIKE public."+name+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	_, err = tx.Exec(ctx, `
 INSERT INTO nodes (id,public_key,node_type,name) VALUES
 ('00000000-0000-0000-0000-000000000001',decode('aabbccdd11223344'||repeat('00',24),'hex'),1,'Companion'),
 ('00000000-0000-0000-0000-000000000002',decode('aabbccdd55667788'||repeat('00',24),'hex'),2,'Repeater');
 INSERT INTO node_short_ids (node_id,iata,prefix_4) VALUES
 ('00000000-0000-0000-0000-000000000001','YKF','\xaabbccdd'),
 ('00000000-0000-0000-0000-000000000002','YKF','\xaabbccdd'),
 ('00000000-0000-0000-0000-000000000001','YHM','\xaabbccdd');`)
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	for _, width := range []int{1, 2, 4, 8} {
		full := "aabbccdd11223344"
		h := full[:width*2]
		b, _ := hex.DecodeString(h)
		candidates, err := store.traceCandidates(ctx, []string{"YKF", "YHM"}, [][]byte{b})
		if err != nil {
			t.Fatal(err)
		}
		flags := byte(0)
		switch width {
		case 2:
			flags = 1
		case 4:
			flags = 2
		case 8:
			flags = 3
		}
		route := traceRoute(&tracePayload{Flags: flags, PathHashes: []string{h}}, []string{"YKF", "YHM"}, candidates)
		count := 2
		confidence := "ambiguous"
		if width == 8 {
			count = 1
			confidence = "high"
		}
		if len(route) != 1 || len(route[0].Nodes) != count || route[0].Confidence != confidence {
			t.Fatalf("width %d: %+v", width, route)
		}
		if route[0].Nodes[0].PublicKey[:16] != "aabbccdd11223344" {
			t.Fatal("companion excluded or eight-byte hash truncated")
		}
	}
}
