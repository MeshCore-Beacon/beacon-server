// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
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
	created, err := ingest.StoreGroupText(ctx, store, k1, hash, payload, "YUL", false, false)
	if err != nil || created == nil || created.Message == nil {
		t.Fatalf("first hearing: %+v, %v", created, err)
	}
	m := created.Message.ID

	// The key is gone; the next hearing cannot decrypt.
	observe("00000000-0000-0000-0000-000000000002", "YOW")
	result, err := ingest.StoreGroupText(ctx, store, keys{}, hash, payload, "YOW", false, false)
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
	// The exact check above just passed, so a reader trusting the read model serves the site page from it.
	trust := db.NewTrust("cmi_decrypt_failure_test", true, db.CmiTrustPolicy)
	trust.Earn()
	reader := db.New(pool, pool, 0, 0, db.WithReadModels(&db.ReadModels{Cmi: trust}))
	channelID := int32(created.ChannelID)
	page, err := reader.ListChannelMessages(ctx, &channelID, time.Time{}, 50, []string{"YOW"}, "", 0)
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != m {
		t.Fatalf("YOW page = %+v, %v", page, err)
	}
}

type noScopes struct{}

func (noScopes) Entries() []scopestore.Entry { return nil }

// A clean ingest run, four workers hearing the same packets at once, leaves nothing for the reconcile: every site
// is recorded by the writers themselves. Reconciles run beside the workers too; what they repair is logged.
func TestCmiCleanIngestReconcilesNothingPostgres(t *testing.T) {
	ctx, pool := migratedPool(t)
	store := db.New(pool, pool, 0, 0)
	h := hub.New()
	go h.Run()
	psk := make([]byte, 16)
	k := keys{0x11: {{Key: psk, Fingerprint: []byte{1}, Name: "fixture"}}}
	c, ok, err := store.BeginCmi(ctx)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	defer c.Close(context.Background())
	if _, err := c.Init(ctx); err != nil {
		t.Fatal(err)
	}

	type hearing struct {
		site, observer string
		env            []byte
	}
	var hearings []hearing
	sites := []string{"YUL", "YOW", "YYZ", "YVR"}
	for i := range 40 {
		ch := byte(0x11)
		if i%3 == 2 {
			ch = 0x22 // no key: hash-only, no message
		}
		g, err := (&meshcore.GroupTextPayload{Timestamp: uint32(1000 + i), Sender: "s", Text: fmt.Sprint("m", i)}).Encrypt(ch, psk)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := g.ToBytes()
		if err != nil {
			t.Fatal(err)
		}
		raw, err := (&meshcore.Packet{Header: meshcore.MakeHeader(meshcore.RouteTypeFlood, meshcore.PayloadTypeGrpTxt, 0), Payload: payload}).ToBytes()
		if err != nil {
			t.Fatal(err)
		}
		env, _ := json.Marshal(map[string]string{"raw": hex.EncodeToString(raw), "timestamp": time.Now().UTC().Format("2006-01-02T15:04:05.000000")})
		for o, site := range sites {
			hearings = append(hearings, hearing{site, fmt.Sprintf("%02x%062x", o+1, i), env})
		}
	}
	rand.New(rand.NewPCG(7, 7)).Shuffle(len(hearings), func(a, b int) { hearings[a], hearings[b] = hearings[b], hearings[a] })

	work := make(chan hearing)
	var wg sync.WaitGroup
	for range 4 {
		w := ingest.New(ingest.Config{BrokerName: "test"}, store, h, k, noScopes{})
		wg.Go(func() {
			for hr := range work {
				w.HandlePacketForTest(ctx, hr.site, hr.observer, hr.env)
			}
		})
	}
	stop := make(chan struct{})
	var during int64
	var runs int
	reconciled := make(chan struct{})
	go func() {
		defer close(reconciled)
		for {
			select {
			case <-stop:
				return
			default:
			}
			n, err := c.Reconcile(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			during += n
			runs++
		}
	}()
	for _, hr := range hearings {
		work <- hr
	}
	close(work)
	wg.Wait()
	close(stop)
	<-reconciled
	t.Logf("%d reconciles beside 4 workers and %d hearings repaired %d pairs", runs, len(hearings), during)

	// After the run, a reconcile (both overlapping ranges) finds nothing to repair, and the table is exact.
	for range 2 {
		if n, err := c.Reconcile(ctx); err != nil || n != 0 {
			t.Fatalf("reconcile after a clean ingest run repaired %d (%v)", n, err)
		}
	}
	var messages, missing, extra int64
	if err := pool.QueryRow(ctx, `WITH def AS (
  SELECT DISTINCT cm.id AS message_id, po.iata, cm.channel_id, p.scope_id
  FROM channel_messages cm JOIN packets p ON p.packet_hash = cm.packet_hash JOIN packet_observations po ON po.packet_hash = cm.packet_hash
), have AS (SELECT message_id, iata, channel_id, scope_id FROM channel_message_iatas)
SELECT (SELECT count(*) FROM channel_messages), (SELECT count(*) FROM (SELECT * FROM def EXCEPT SELECT * FROM have) m), (SELECT count(*) FROM (SELECT * FROM have EXCEPT SELECT * FROM def) x)`).Scan(&messages, &missing, &extra); err != nil {
		t.Fatal(err)
	}
	if messages == 0 || missing != 0 || extra != 0 {
		t.Fatalf("%d messages, missing %d, extra %d", messages, missing, extra)
	}
}

// The background decrypt backfill and live hearings of the same stored packets race: each packet still gets one
// message, its sites are exact (the seed and the live hearing's Add cover each other), and the packets' hours are
// queued for a re-roll whichever side stored the message.
func TestDecryptBackfillRacesLiveHearingsPostgres(t *testing.T) {
	ctx, pool := migratedPool(t)
	store := db.New(pool, pool, 0, 0)
	psk := make([]byte, 16)
	if _, err := pool.Exec(ctx, `
INSERT INTO iata_codes (iata) VALUES ('YUL'), ('YOW') ON CONFLICT DO NOTHING;
INSERT INTO observers (id, public_key) VALUES ('00000000-0000-0000-0000-000000000001', '\x01'), ('00000000-0000-0000-0000-000000000002', '\x02')`); err != nil {
		t.Fatal(err)
	}
	const n = 60
	type stored struct{ hash, payload []byte }
	var packets []stored
	for i := range n {
		grpTxt, err := (&meshcore.GroupTextPayload{Timestamp: uint32(1000 + i), Sender: "Fixture", Text: fmt.Sprint("message ", i)}).Encrypt(0x11, psk)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := grpTxt.ToBytes()
		if err != nil {
			t.Fatal(err)
		}
		hash := []byte(fmt.Sprintf("race-%02d", i))
		if _, err := store.UpsertPacket(ctx, ingest.UpsertPacketParams{PacketHash: hash, PayloadType: 5, RouteType: 1, RawHeader: []byte{0x15}, RawPayload: payload, ChannelHash: []byte{0x11}}); err != nil {
			t.Fatal(err)
		}
		// Stored before the key was known: hash-only, undecrypted, heard at YUL two hours ago.
		if _, _, err := store.InsertObservation(ctx, ingest.InsertObservationParams{PacketHash: hash, ObserverID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), IATA: "YUL", HeardAt: time.Now().Add(-2 * time.Hour), HashSize: 1, PayloadType: 5}); err != nil {
			t.Fatal(err)
		}
		packets = append(packets, stored{hash, payload})
	}
	k := keys{0x11: {{Key: psk, Fingerprint: []byte{1}, Name: "fixture"}}}

	runCtx, stop := context.WithCancel(ctx)
	backfill := ingest.NewDecryptBackfill(store, k, nil)
	done := make(chan struct{})
	go func() { backfill.Run(runCtx); close(done) }()
	backfill.RequestAll()
	var live sync.WaitGroup
	live.Go(func() {
		for _, p := range packets {
			if _, _, err := store.InsertObservation(ctx, ingest.InsertObservationParams{PacketHash: p.hash, ObserverID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), IATA: "YOW", HeardAt: time.Now(), HashSize: 1, PayloadType: 5}); err != nil {
				t.Error(err)
				return
			}
			if _, err := ingest.StoreGroupText(ctx, store, k, p.hash, p.payload, "YOW", false, true); err != nil {
				t.Error(err)
				return
			}
		}
	})
	live.Wait()
	deadline := time.Now().Add(time.Minute)
	for {
		var left int
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM packets WHERE decrypted IS NOT TRUE").Scan(&left); err != nil {
			t.Fatal(err)
		}
		if left == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d packets still undecrypted", left)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	<-done

	var messages, distinct, dirty int
	if err := pool.QueryRow(ctx, "SELECT count(*), count(DISTINCT packet_hash), (SELECT count(*) FROM analytics_dirty_hours WHERE hour = date_trunc('hour', now() - interval '2 hours', 'UTC')) FROM channel_messages").Scan(&messages, &distinct, &dirty); err != nil {
		t.Fatal(err)
	}
	if messages != n || distinct != n || dirty != 1 {
		t.Fatalf("%d messages for %d packets (want %d each), early hour queued %d times (want 1)", messages, distinct, n, dirty)
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
		t.Fatalf("channel message sites: missing %d, extra %d", missing, extra)
	}
}
