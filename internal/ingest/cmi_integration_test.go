// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/meshcore-go/meshcore-go"
)

type keys map[byte][]keystore.Entry

func (k keys) GetKey(hash []byte) []keystore.Entry {
	if len(hash) == 0 {
		return nil
	}
	return k[hash[0]]
}

// migratedPool returns a pool on a fresh schema built by db.RunMigrations, dropped on cleanup.
func migratedPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := pgx.Identifier{"cmi_ingest_" + strings.ReplaceAll(uuid.NewString(), "-", "")}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
		admin.Close(context.Background())
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return ctx, pool
}

// A later hearing whose decrypt fails (the key was dropped at runtime) still records its site at once.
func TestCmiAddOnDecryptFailurePostgres(t *testing.T) {
	ctx, pool := migratedPool(t)
	store := db.New(pool, pool, 0, 0)
	psk := make([]byte, 16)
	grpTxt, err := (&meshcore.GroupTextPayload{Timestamp: 1000, Sender: "Fixture", Text: "test"}).Encrypt(0x11, psk)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := grpTxt.ToBytes()
	if err != nil {
		t.Fatal(err)
	}
	hash := []byte("decrypt-failure")
	if _, err := pool.Exec(ctx, `
SELECT setval('channel_messages_id_seq', 3000000000);
SELECT setval('packet_observations_id_seq', 3000000000);
INSERT INTO iata_codes (iata) VALUES ('YUL'), ('YOW') ON CONFLICT DO NOTHING;
INSERT INTO observers (id, public_key) VALUES ('00000000-0000-0000-0000-000000000001', '\x01'), ('00000000-0000-0000-0000-000000000002', '\x02')`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertPacket(ctx, ingest.UpsertPacketParams{PacketHash: hash, PayloadType: 5, RouteType: 1, RawHeader: []byte{0x15}, RawPayload: payload, ChannelHash: []byte{0x11}}); err != nil {
		t.Fatal(err)
	}
	observe := func(observer, iata string) {
		t.Helper()
		if _, _, err := store.InsertObservation(ctx, ingest.InsertObservationParams{PacketHash: hash, ObserverID: uuid.MustParse(observer), IATA: iata, HeardAt: time.Now(), HashSize: 1, PayloadType: 5}); err != nil {
			t.Fatal(err)
		}
	}

	k1 := keys{0x11: {{Key: psk, Fingerprint: []byte{1}, Name: "fixture"}}}
	observe("00000000-0000-0000-0000-000000000001", "YUL")
	created, err := ingest.StoreGroupText(ctx, store, k1, hash, payload, "YUL")
	if err != nil || created == nil || created.Message == nil {
		t.Fatalf("first hearing: %+v, %v", created, err)
	}
	m := created.Message.ID

	// The key is gone; the next hearing cannot decrypt.
	observe("00000000-0000-0000-0000-000000000002", "YOW")
	result, err := ingest.StoreGroupText(ctx, store, keys{}, hash, payload, "YOW")
	if result != nil || err != nil {
		t.Fatalf("keyless hearing: %+v, %v", result, err)
	}

	var sites []string
	if err := pool.QueryRow(ctx, "SELECT array_agg(iata::text ORDER BY iata) FROM channel_message_iatas WHERE message_id = $1", m).Scan(&sites); err != nil {
		t.Fatal(err)
	}
	if strings.Join(sites, ",") != "YOW,YUL" {
		t.Fatalf("sites = %v, want YOW and YUL", sites)
	}
	var missing, extra int64
	if err := pool.QueryRow(ctx, `WITH def AS (
  SELECT DISTINCT cm.id AS message_id, po.iata, cm.channel_id, p.scope_id
  FROM channel_messages cm JOIN packets p ON p.packet_hash = cm.packet_hash JOIN packet_observations po ON po.packet_hash = cm.packet_hash
), have AS (SELECT message_id, iata, channel_id, scope_id FROM channel_message_iatas)
SELECT (SELECT count(*) FROM (SELECT * FROM def EXCEPT SELECT * FROM have) m), (SELECT count(*) FROM (SELECT * FROM have EXCEPT SELECT * FROM def) x)`).Scan(&missing, &extra); err != nil {
		t.Fatal(err)
	}
	if missing != 0 || extra != 0 {
		t.Fatalf("missing %d, extra %d", missing, extra)
	}
	channelID := int32(created.ChannelID)
	page, err := store.ListChannelMessages(ctx, &channelID, time.Time{}, 50, []string{"YOW"}, "", 0)
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != m {
		t.Fatalf("YOW page = %+v, %v", page, err)
	}
}
