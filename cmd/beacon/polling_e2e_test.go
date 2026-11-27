// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/router"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// e2eSchemaPool opens a pool on a fresh schema and migrates it.
func e2eSchemaPool(t *testing.T) (context.Context, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for PostgreSQL regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	setup, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "polling_e2e_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := setup.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := setup.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
		setup.Close(context.Background())
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

type e2eClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *e2eClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *e2eClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func TestPollingEndToEndPostgres(t *testing.T) {
	ctx, pool := e2eSchemaPool(t)
	if _, err := pool.Exec(ctx, `INSERT INTO iata_codes (iata) VALUES ('YVR')`); err != nil {
		t.Fatal(err)
	}
	store := db.New(pool, pool, 0, 0)
	clock := &e2eClock{t: time.Now().UTC().Truncate(time.Second)}
	cfg, err := config.PollingConfig{Enabled: true, MQTTHosts: []string{"mqtt.example.net"}}.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	svc := polling.NewService(store, cfg, clock.Now, slog.New(slog.DiscardHandler))
	h := router.New(nil, nil, nil, router.Options{GuestPolling: svc})

	o1, o2 := bytes.Repeat([]byte{0x01}, 32), bytes.Repeat([]byte{0x02}, 32)
	x := bytes.Repeat([]byte{0xAA}, 32)
	hex1, hex2, hexX := hex.EncodeToString(o1), hex.EncodeToString(o2), hex.EncodeToString(x)

	get := func(path string) (int, []byte) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.RemoteAddr = "198.51.100.1:1"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Body.Len() >= 6144 {
			t.Fatalf("%s: body is %d bytes", path, rec.Body.Len())
		}
		return rec.Code, rec.Body.Bytes()
	}
	policy := func() string {
		t.Helper()
		code, body := get("/api/guest-polling/policy")
		if code != 200 || !strings.Contains(string(body), `"reading_format":"telemetry"`) {
			t.Fatalf("policy: %d %s", code, body)
		}
		return string(body)
	}
	owners := func(obs string) polling.OwnersAnswer {
		t.Helper()
		code, body := get("/api/guest-polling/owners?observer=" + obs + "&client=meshcore.observer_fw")
		if code != 200 {
			t.Fatalf("owners: %d %s", code, body)
		}
		var a polling.OwnersAnswer
		if err := json.Unmarshal(body, &a); err != nil {
			t.Fatal(err)
		}
		return a
	}
	elect := func() {
		t.Helper()
		if err := svc.ElectAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	claim := func(obs []byte, misses int) {
		t.Helper()
		raw := fmt.Sprintf(`{"v":2,"origin":"Obs","origin_id":%q,"timestamp":%q,"plugin":"meshcore.observer_fw/1.0",
"policy_version":"p1","community":true,"accepts_requests":false,"request_slots":0,
"nodes":[{"node_id":%q,"reply_hops":0,"via":null,"key":"own","snr":6.5,"state":"ok","misses":%d}]}`,
			strings.ToUpper(hex.EncodeToString(obs)), clock.Now().Format(time.RFC3339), hexX, misses)
		svc.HandleClaim(ctx, strings.ToUpper(hex.EncodeToString(obs)), []byte(raw))
	}
	reading := func(obs []byte, tag int, sampled time.Time) {
		t.Helper()
		id := strings.ToUpper(hex.EncodeToString(obs))
		raw := fmt.Sprintf(`{"v":2,"origin":"Obs","origin_id":%q,"timestamp":%q,"plugin":"meshcore.observer_fw/1.0",
"readings":[{"node_id":%q,"battery_mv":4038,"source":"poll","reply_hops":0,"method":"status","sampled_at":%q,"snr":6.5,
"stats":{"uptime_secs":788089,"noise_floor":-116},"tag":%d,"packet_hash":"0123456789ABCDEF"}]}`,
			id, clock.Now().Format(time.RFC3339), hexX, sampled.Format(time.RFC3339), tag)
		svc.HandleReading(ctx, id, []byte(raw))
	}
	readingRows := func() int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM repeater_readings`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// 1. Policy before any data, and the key routes always answer 404.
	if body := policy(); !strings.Contains(body, `"paused":false`) {
		t.Fatalf("enabled polling starts paused: %s", body)
	}
	for _, c := range [][2]string{{http.MethodPost, "/api/guest-keys"}, {http.MethodGet, "/api/shared-guest/key"}} {
		req := httptest.NewRequest(c[0], c[1], nil)
		req.RemoteAddr = "198.51.100.1:1"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 404 {
			t.Fatalf("%s %s: %d", c[0], c[1], rec.Code)
		}
	}

	// 2. Zero-hop adverts from X: O1 at 9 dB, O2 at 3 dB, four each.
	id1, err := store.ObserverID(ctx, o1)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := store.ObserverID(ctx, o2)
	if err != nil {
		t.Fatal(err)
	}
	hear := func(obs uuid.UUID, snr float32) {
		if _, err := pool.Exec(ctx, `WITH p AS (
INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,origin_pubkey,raw_payload,raw_header,first_heard_at,last_heard_at)
VALUES (decode(md5(gen_random_uuid()::text),'hex'),4,0,1,$1,'\x00','\x00',$2,$2) RETURNING packet_hash)
INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type,rssi,snr)
SELECT packet_hash,$3,'YVR',$2,0,1,0,4,-90,$4 FROM p`, x, clock.Now(), obs, snr); err != nil {
			t.Fatal(err)
		}
	}
	for range 4 {
		hear(id1, 9)
		hear(id2, 3)
	}

	// 3. Both observers claim X.
	claim(o1, 0)
	claim(o2, 0)
	elect()

	// 4. O1 polls, O2 stands by.
	if e := owners(hex1).Nodes[hexX]; e.Role != "polling" || !e.Mine || e.LatestTS != nil {
		t.Fatalf("o1 owners: %+v", e)
	}
	if e := owners(hex2).Nodes[hexX]; e.Role != "standby" || e.StandbyRank != 1 {
		t.Fatalf("o2 owners: %+v", e)
	}

	// 5. A reading is stored once, even when it arrives twice, and sets latest_ts.
	sampled := clock.Now().Add(-time.Minute)
	reading(o1, 1794484800, sampled)
	reading(o1, 1794484800, sampled)
	if n := readingRows(); n != 1 {
		t.Fatalf("readings: %d", n)
	}
	e := owners(hex2).Nodes[hexX]
	if e.LatestTS == nil || *e.LatestTS != sampled.Format(time.RFC3339) {
		t.Fatalf("latest_ts: %v, want %s", e.LatestTS, sampled.Format(time.RFC3339))
	}

	// 6. O1 reports three misses: it drops to standby and O2 takes over.
	clock.Advance(2 * time.Minute)
	claim(o1, polling.MissLimit)
	elect()
	if e := owners(hex2).Nodes[hexX]; e.Role != "polling" || !e.Mine {
		t.Fatalf("o2 after misses: %+v", e)
	}
	if e := owners(hex1).Nodes[hexX]; e.Role != "standby" || e.StandbyRank != 1 {
		t.Fatalf("o1 after misses: %+v", e)
	}

	// 6b. Misses back to 0: O1 beats O2 by 6 dB and takes the job back.
	clock.Advance(2 * time.Minute)
	claim(o1, 0)
	elect()
	if e := owners(hex1).Nodes[hexX]; e.Role != "polling" || !e.Mine {
		t.Fatalf("o1 after recovery: %+v", e)
	}

	// 7. An operator blocks O2: its readings are dropped and its owners answer is empty.
	if err := store.SetPollingObserver(ctx, o2, true, false, "e2e"); err != nil {
		t.Fatal(err)
	}
	reading(o2, 1794485000, clock.Now().Add(-time.Minute))
	if n := readingRows(); n != 1 {
		t.Fatalf("blocked reading stored: %d rows", n)
	}
	a := owners(hex2)
	if !a.Policy.ObserverBlocked || len(a.Nodes) != 0 {
		t.Fatalf("blocked owners: %+v", a)
	}

	// 8. Claims expire with no refresh.
	clock.Advance(polling.ClaimTTL + time.Minute)
	elect()
	for _, o := range []string{hex1, hex2} {
		if n := len(owners(o).Nodes); n != 0 {
			t.Fatalf("owners %s after expiry: %d nodes", o[:4], n)
		}
	}
	policy()
}
