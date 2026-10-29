// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build unix

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/meshmapper"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const readOnlyBootAPIKey = "read-only-boot-test-key"

// readOnlyBootConfig makes a normal boot write: Seed upserts the IATA, region and hashtag
// channel, the disabled MeshMapper imports clear their rows, and the jobs run at once.
const readOnlyBootConfig = `server:
  read_only: %t
ratelimit:
  enabled: false
auth:
  api_key: ` + readOnlyBootAPIKey + `
iatas:
  SEA:
    name: Seattle
    lat: 47.45
    lng: -122.31
regions:
  - name: Pacific Northwest
    slug: pnw
    iatas: [SEA]
channel_keys:
  hashtags:
    - readonlytest
%s`

// readOnlyScopesConfig enables the scope import on the read-only boot only, so the control never reaches the network.
const readOnlyScopesConfig = `meshmapper:
  scopes:
    enabled: true
`

// readOnlyCatalogueScope is the only scope in the saved SEA catalogue.
const readOnlyCatalogueScope = "#rocatalogue"

// readOnlyRequests is the request mix a read-only server serves. GETs must not fail with a
// server error; writes must fail and never succeed.
var readOnlyRequests = []struct {
	method, path, body string
	write              bool
}{
	{"GET", "/api/v1/info", "", false},
	{"GET", "/api/v1/iatas", "", false},
	{"GET", "/api/v1/iatas/SEA", "", false},
	{"GET", "/api/v1/regions", "", false},
	{"GET", "/api/v1/nodes?limit=5", "", false},
	{"GET", "/api/v1/observers?limit=5", "", false},
	{"GET", "/api/v1/observers/directory", "", false},
	{"GET", "/api/v1/packets?limit=5", "", false},
	{"GET", "/api/v1/routes?limit=5", "", false},
	{"GET", "/api/v1/routes?iata=SEA&limit=5", "", false},
	{"GET", "/api/v1/channels", "", false},
	{"GET", "/api/v1/messages?limit=5", "", false},
	{"GET", "/api/v1/scopes", "", false},
	{"GET", "/api/v1/brokers", "", false},
	{"GET", "/api/v1/traces", "", false},
	{"GET", "/api/v1/stats/overview", "", false},
	{"GET", "/api/v1/stats/node-types", "", false},
	{"GET", "/api/v1/stats/top-nodes", "", false},
	{"GET", "/api/v1/admin/accounts", "", false},
	{"POST", "/api/v1/admin/accounts", `{"name":"read-only boot"}`, true},
}

// tableStat is a table's live row count and its insert, update and delete counters.
type tableStat struct{ rows, ins, upd, del int64 }

// A read-only server boots against a migrated database, serves a request mix and shuts down
// without changing any table: no row count and no insert, update or delete counter moves.
// The same boot without read_only does move them, so the check can see a write.
func TestReadOnlyBootWritesNothingPostgres(t *testing.T) {
	if os.Getenv("BEACON_TEST_BOOT_MAIN") == "1" {
		// The parent holds stdin open; when it dies, even by SIGKILL, stdin closes and the server goes with it.
		go func() {
			_, _ = io.Copy(io.Discard, os.Stdin)
			os.Exit(3)
		}()
		main()
		return
	}
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := "beacon_read_only_" + strings.ToLower(rand.Text())
	ident := pgx.Identifier{name}.Sanitize()
	_, err = admin.Exec(ctx, "CREATE DATABASE "+ident)
	admin.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if admin, err := pgx.Connect(ctx, dsn); err == nil {
			_, _ = admin.Exec(ctx, "DROP DATABASE "+ident+" WITH (FORCE)")
			admin.Close(ctx)
		}
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"INSERT INTO iata_codes(iata) VALUES ('YVR')",
		"INSERT INTO observers(id, public_key) VALUES ('00000000-0000-0000-0000-000000000001', '\\x01')",
		"INSERT INTO nodes(id, public_key, name, node_type) VALUES ('00000000-0000-0000-0000-0000000000a1', '\\xaa11', 'Alice', 2)",
	} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	pool.Close()
	statsConn := func() *pgx.Conn {
		c, err := pgx.ConnectConfig(ctx, cfg.ConnConfig.Copy())
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	// The control boot writes; it also leaves the database as a server would after a normal start.
	before := snapshotTables(t, ctx, statsConn(), name)
	bootAndServe(t, ctx, poolDSN(t, dsn, name), false)
	after := snapshotTables(t, ctx, statsConn(), name)
	if maps.Equal(before, after) {
		t.Fatal("a normal boot changed no table, so the check cannot see a write")
	}

	seedScopeCatalogue(t, ctx, cfg)
	before = snapshotTables(t, ctx, statsConn(), name)
	log := bootAndServe(t, ctx, poolDSN(t, dsn, name), true)
	after = snapshotTables(t, ctx, statsConn(), name)
	for _, table := range slices.Sorted(maps.Keys(after)) {
		if before[table] != after[table] {
			t.Errorf("%s changed under read_only: before %+v, after %+v", table, before[table], after[table])
		}
	}
	if len(before) != len(after) {
		t.Errorf("table set changed under read_only: %d tables before, %d after", len(before), len(after))
	}
	if !strings.Contains(log, readOnlyWarning) {
		t.Errorf("boot log lacks the read-only warning:\n%s", log)
	}
	// A gated step that still ran would have hit a read-only session and logged it. The only refused write is
	// the request mix's admin POST.
	refused := 0
	for _, line := range strings.Split(log, "\n") {
		if !strings.Contains(line, "25006") && !strings.Contains(line, "read-only transaction") {
			continue
		}
		if strings.Contains(line, `msg="account request failed"`) {
			refused++
			continue
		}
		t.Errorf("read-only server refused a write outside the request mix: %s", line)
	}
	if refused != 1 {
		t.Errorf("the admin POST logged %d refused writes, want 1:\n%s", refused, log)
	}
}

// seedScopeCatalogue saves a validated MeshMapper scope catalogue for SEA, as the writer's importer would.
func seedScopeCatalogue(t *testing.T, ctx context.Context, cfg *pgxpool.Config) {
	t.Helper()
	pool, err := pgxpool.NewWithConfig(ctx, cfg.Copy())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	now := time.Now().UTC().Truncate(time.Second)
	payload := fmt.Sprintf(`{"generated_at":%q,"region":"SEA","zones":["SEA"],"repeaters":1,"scoped":1,`+
		`"scopes":[{"name":%q,"repeaters":1,"default":0,"monitored":false,"wardriving":false}]}`,
		now.Format(time.RFC3339), strings.TrimPrefix(readOnlyCatalogueScope, "#"))
	cache := meshmapper.Cache{Payload: []byte(payload), CheckedAt: now, AttemptedAt: now, NextAttempt: now.Add(time.Hour)}
	store := db.New(pool, pool, 0, 0)
	if err := store.SaveScopeCatalogue(ctx, "SEA", "https://sea.example/get_scopes.php", cache,
		[]scopestore.Entry{scopestore.FromName(readOnlyCatalogueScope)}); err != nil {
		t.Fatal(err)
	}
}

// poolDSN points dsn at the named database.
func poolDSN(t *testing.T, dsn, name string) string {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("postgres://%s@%s/%s?sslmode=disable", userInfo(cfg), net.JoinHostPort(cfg.Host, fmt.Sprint(cfg.Port)), name)
}

func userInfo(cfg *pgx.ConnConfig) string {
	if cfg.Password == "" {
		return cfg.User
	}
	return cfg.User + ":" + cfg.Password
}

// bootAndServe runs the server in a child process, sends the request mix once it is ready,
// stops it with SIGTERM and returns its log.
func bootAndServe(t *testing.T, ctx context.Context, dsn string, readOnly bool) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	extra := ""
	if readOnly {
		extra = readOnlyScopesConfig
	}
	if err := os.WriteFile(path, []byte(fmt.Sprintf(readOnlyBootConfig, readOnly, extra)), 0600); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()

	cmd := exec.Command(os.Args[0], "-test.run=^TestReadOnlyBootWritesNothingPostgres$")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "BEACON_TEST_BOOT_MAIN=1", "CONFIG_PATH="+path, "LISTEN_ADDR="+addr,
		"POSTGRES_DSN="+dsn, "BEACON_API_KEY="+readOnlyBootAPIKey, "MESHMAPPER_API_KEY=", "REDIS_ADDR=",
		"LOG_FORMAT=text", "LOG_LEVEL=info")
	var out lockedBuffer
	cmd.Stdout, cmd.Stderr = &out, &out
	// Its own process group, so cleanup reaches anything it started.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	stopped := false
	defer func() {
		if !stopped {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}
		stdin.Close()
	}()

	base := "http://" + addr
	client := &http.Client{Timeout: 10 * time.Second}
	ready := time.Now().Add(60 * time.Second)
	for {
		resp, err := client.Get(base + "/api/v1/info")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case err := <-done:
			stopped = true
			t.Fatalf("server exited before it was ready (read_only=%t): %v\n%s", readOnly, err, out.String())
		default:
		}
		if time.Now().After(ready) {
			t.Fatalf("server not ready (read_only=%t):\n%s", readOnly, out.String())
		}
		time.Sleep(100 * time.Millisecond)
	}

	if readOnly {
		for _, r := range readOnlyRequests {
			req, err := http.NewRequestWithContext(ctx, r.method, base+r.path, strings.NewReader(r.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer "+readOnlyBootAPIKey)
			if r.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("%s %s: %v", r.method, r.path, err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			switch {
			case r.write && resp.StatusCode < 300:
				t.Errorf("%s %s succeeded under read_only: %d %s", r.method, r.path, resp.StatusCode, body)
			case !r.write && resp.StatusCode >= 500:
				t.Errorf("%s %s: %d %s", r.method, r.path, resp.StatusCode, body)
			}
		}
		// Region scope lists come from the restored catalogue membership.
		resp, err := client.Get(base + "/api/v1/scopes?region=pnw")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), readOnlyCatalogueScope) {
			t.Errorf("GET /api/v1/scopes?region=pnw: %d %s, want %s from the saved catalogue", resp.StatusCode, body, readOnlyCatalogueScope)
		}
	} else {
		// Give the boot-time jobs a moment to write before the control stops.
		time.Sleep(2 * time.Second)
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		stopped = true
		if err != nil {
			t.Fatalf("server exit (read_only=%t): %v\n%s", readOnly, err, out.String())
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("server did not stop (read_only=%t):\n%s", readOnly, out.String())
	}
	return out.String()
}

// snapshotTables reads every user table's row count and tuple counters once the server's
// sessions have ended; a backend flushes its statistics as it exits.
func snapshotTables(t *testing.T, ctx context.Context, conn *pgx.Conn, database string) map[string]tableStat {
	t.Helper()
	defer conn.Close(context.Background())
	waitSessionsGone(t, ctx, conn, database)
	// Statistics arrive as backends flush them; read until two snapshots in a row agree.
	prev := readTables(t, ctx, conn)
	for deadline := time.Now().Add(30 * time.Second); ; {
		time.Sleep(300 * time.Millisecond)
		cur := readTables(t, ctx, conn)
		if maps.Equal(prev, cur) {
			return cur
		}
		if time.Now().After(deadline) {
			t.Fatal("table statistics did not settle")
		}
		prev = cur
	}
}

func waitSessionsGone(t *testing.T, ctx context.Context, conn *pgx.Conn, database string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var n int
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND application_name LIKE 'beacon-%'", database).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d server sessions still open", n)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// readTables reads every user table's row count and tuple counters.
func readTables(t *testing.T, ctx context.Context, conn *pgx.Conn) map[string]tableStat {
	t.Helper()
	if _, err := conn.Exec(ctx, "SELECT pg_stat_clear_snapshot()"); err != nil {
		t.Fatal(err)
	}
	rows, err := conn.Query(ctx, "SELECT schemaname, relname, n_tup_ins, n_tup_upd, n_tup_del FROM pg_stat_user_tables")
	if err != nil {
		t.Fatal(err)
	}
	stats := map[string]tableStat{}
	for rows.Next() {
		var schema, rel string
		var s tableStat
		if err := rows.Scan(&schema, &rel, &s.ins, &s.upd, &s.del); err != nil {
			t.Fatal(err)
		}
		stats[pgx.Identifier{schema, rel}.Sanitize()] = s
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(stats) == 0 {
		t.Fatal("no user tables")
	}
	for table, s := range stats {
		if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&s.rows); err != nil {
			t.Fatal(err)
		}
		stats[table] = s
	}
	return stats
}

// lockedBuffer collects a child's output from two pipes.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
