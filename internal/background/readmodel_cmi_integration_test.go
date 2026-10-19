// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// statementNames records the sqlc name of every statement run through it.
type statementNames struct {
	sqlc.DBTX
	names []string
}

func (s *statementNames) note(sql string) {
	name, _, _ := strings.Cut(strings.TrimPrefix(sql, "-- name: "), " ")
	s.names = append(s.names, name)
}

func (s *statementNames) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	s.note(sql)
	return s.DBTX.Exec(ctx, sql, args...)
}

func (s *statementNames) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	s.note(sql)
	return s.DBTX.Query(ctx, sql, args...)
}

func (s *statementNames) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	s.note(sql)
	return s.DBTX.QueryRow(ctx, sql, args...)
}

func cmiJobsFor(store *db.Store, tr *db.Trust) *CmiJobs {
	return &CmiJobs{
		Begin: func(ctx context.Context) (CmiSession, bool, error) {
			s, ok, err := store.BeginCmi(ctx)
			if s == nil {
				return nil, ok, err
			}
			return s, ok, err
		},
		Wait:     func(context.Context) error { return nil },
		Trust:    tr,
		Interval: 10 * time.Millisecond,
	}
}

func bootWithin(t *testing.T, ctx context.Context, j *CmiJobs, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() { j.Boot(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("boot did not finish")
	}
	if !j.Trust.Trusted() {
		t.Fatal("not trusted after the boot")
	}
}

// cmiSite writes one group-text packet heard at sites (ids above 3,000,000,000), its message on channel 7, and the
// later hearings through the Add, as the live writer does.
func cmiSite(t *testing.T, ctx context.Context, store *db.Store, hash string, observers map[string]uuid.UUID, sites ...string) {
	t.Helper()
	if _, err := store.UpsertPacket(ctx, ingest.UpsertPacketParams{PacketHash: []byte(hash), PayloadType: 5, RouteType: 1, RawHeader: []byte{0x15}, RawPayload: []byte(hash)}); err != nil {
		t.Fatal(err)
	}
	for i, site := range sites {
		if _, _, err := store.InsertObservation(ctx, ingest.InsertObservationParams{PacketHash: []byte(hash), ObserverID: observers[site], IATA: site, HeardAt: time.Now(), HashSize: 1, PayloadType: 5}); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if _, err := store.InsertChannelMessage(ctx, ingest.InsertChannelMessageParams{ChannelID: 7, PacketHash: []byte(hash), SentAt: time.Now()}); err != nil {
				t.Fatal(err)
			}
		} else if err := store.AddChannelMessageIATA(ctx, []byte(hash), site); err != nil {
			t.Fatal(err)
		}
	}
}

func cmiPrepare(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]uuid.UUID {
	t.Helper()
	observers := map[string]uuid.UUID{}
	if _, err := pool.Exec(ctx, `SELECT setval('channel_messages_id_seq', 3000000000);
SELECT setval('packet_observations_id_seq', 3000000000);
INSERT INTO iata_codes (iata) VALUES ('YUL'), ('YOW'), ('YYZ') ON CONFLICT DO NOTHING;
INSERT INTO channels (id, channel_hash, name) VALUES (7, '\x11', 'ch7')`); err != nil {
		t.Fatal(err)
	}
	for _, site := range []string{"YUL", "YOW", "YYZ"} {
		observers[site] = uuid.New()
		if _, err := pool.Exec(ctx, "INSERT INTO observers (id, public_key) VALUES ($1, $2)", observers[site], []byte(site)); err != nil {
			t.Fatal(err)
		}
	}
	return observers
}

func missingPairs(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM (
  SELECT DISTINCT cm.id, po.iata, cm.channel_id, p.scope_id
  FROM channel_messages cm JOIN packets p ON p.packet_hash = cm.packet_hash JOIN packet_observations po ON po.packet_hash = cm.packet_hash
  EXCEPT SELECT message_id, iata, channel_id, scope_id FROM channel_message_iatas) m`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCmiBootRepairPostgres(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		ctx, pool := migratedPool(t)
		tr := db.NewTrust("cmi", true, db.CmiTrustPolicy)
		bootWithin(t, ctx, cmiJobsFor(db.New(pool, pool, 0, 0), tr), 30*time.Second)
	})

	ctx, pool := migratedPool(t)
	maint := db.New(pool, pool, 0, 0)
	observers := cmiPrepare(t, ctx, pool)
	for i := range 20 {
		cmiSite(t, ctx, maint, fmt.Sprintf("p%02d", i), observers, []string{"YUL", "YOW", "YYZ"}[:1+i%3]...)
	}
	tr := db.NewTrust("cmi", true, db.CmiTrustPolicy)
	bootWithin(t, ctx, cmiJobsFor(maint, tr), 30*time.Second)
	rec := &statementNames{DBTX: pool}
	trusted := db.New(rec, nil, 0, 0, db.WithReadModels(&db.ReadModels{Cmi: tr}))
	ch := int32(7)
	if _, err := trusted.ListChannelMessages(ctx, &ch, time.Time{}, 50, []string{"YUL"}, "", 0); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(rec.names, ","); got != "ListChannelMessagesByIATAs" {
		t.Fatalf("trusted store ran %s", got)
	}

	// An older binary, while this one was down: key-import messages for old packets without sites, and hearings
	// stored without their Add.
	for i := range 3 {
		hash := []byte(fmt.Sprintf("old%02d", i))
		if _, err := maint.UpsertPacket(ctx, ingest.UpsertPacketParams{PacketHash: hash, PayloadType: 5, RouteType: 1, RawHeader: []byte{0x15}, RawPayload: hash}); err != nil {
			t.Fatal(err)
		}
		for _, site := range []string{"YUL", "YOW"} {
			if _, _, err := maint.InsertObservation(ctx, ingest.InsertObservationParams{PacketHash: hash, ObserverID: observers[site], IATA: site, HeardAt: time.Now(), HashSize: 1, PayloadType: 5}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := pool.Exec(ctx, "INSERT INTO channel_messages (channel_id, packet_hash, sent_at) VALUES (7, $1, now())", hash); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 4 {
		if _, _, err := maint.InsertObservation(ctx, ingest.InsertObservationParams{PacketHash: []byte(fmt.Sprintf("p%02d", i*3)), ObserverID: observers["YYZ"], IATA: "YYZ", HeardAt: time.Now(), HashSize: 1, PayloadType: 5}); err != nil {
			t.Fatal(err)
		}
	}
	missing := missingPairs(t, ctx, pool)
	if missing != 10 {
		t.Fatalf("the older binary left %d pairs missing, want 10", missing)
	}

	// The next boot: untrusted until its repair finishes, then every page equals today's statement.
	tr2 := db.NewTrust("cmi", true, db.CmiTrustPolicy)
	before := repairedRows("cmi")
	if tr2.Trusted() {
		t.Fatal("trusted before the boot")
	}
	j := cmiJobsFor(maint, tr2)
	bootWithin(t, ctx, j, 30*time.Second)
	if got := repairedRows("cmi") - before; got != float64(missing) {
		t.Fatalf("repaired counter grew by %v, want %d", got, missing)
	}
	if n := missingPairs(t, ctx, pool); n != 0 {
		t.Fatalf("%d pairs still missing", n)
	}
	served := db.New(pool, nil, 0, 0, db.WithReadModels(&db.ReadModels{Cmi: tr2}))
	today := db.New(pool, nil, 0, 0)
	for _, sites := range [][]string{{"YUL"}, {"YOW"}, {"YYZ"}, {"YUL", "YOW", "YYZ"}} {
		for _, chID := range []*int32{&ch, nil} {
			got, err := served.ListChannelMessages(ctx, chID, time.Time{}, 50, sites, "", 0)
			if err != nil {
				t.Fatal(err)
			}
			want, err := today.ListChannelMessages(ctx, chID, time.Time{}, 50, sites, "", 0)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) || len(got.Items) == 0 {
				t.Fatalf("%v: %+v, today %+v", sites, got, want)
			}
		}
	}
	before = repairedRows("cmi")
	if err := j.Task().Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got := repairedRows("cmi") - before; got != 0 || !tr2.Trusted() {
		t.Fatalf("the next run repaired %v, trusted %v", got, tr2.Trusted())
	}
}

func TestCmiReadOnlyTrustPostgres(t *testing.T) {
	ctx, pool := migratedPool(t)
	maint := db.New(pool, pool, 0, 0)
	observers := cmiPrepare(t, ctx, pool)
	for i := range 10 {
		cmiSite(t, ctx, maint, fmt.Sprintf("p%02d", i), observers, "YUL", "YOW")
	}
	cfg := pool.Config().Copy()
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	ro, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	roStore := db.New(ro, ro, 0, 0)
	stateRow := func() string {
		var v string
		if err := pool.QueryRow(ctx, "SELECT coalesce((SELECT value::text || updated_at::text FROM background_state WHERE key = 'cmi'), '')").Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	check := func(label string, want bool) {
		t.Helper()
		before := stateRow()
		tr := db.NewTrust("cmi", true, db.CmiTrustPolicy)
		CmiReadOnlyTrust(ctx, roStore.CheckCmiReadOnly, tr)
		if tr.Trusted() != want {
			t.Fatalf("%s: trusted %v, want %v", label, tr.Trusted(), want)
		}
		if stateRow() != before {
			t.Fatalf("%s: the read-only check wrote the state", label)
		}
	}
	check("no state", false)

	// Complete, never verified.
	c, ok, err := maint.BeginCmi(ctx)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if _, err := c.Init(ctx); err != nil {
		t.Fatal(err)
	}
	st := c.State()
	st.Complete = true
	if err := maint.SaveState(ctx, "cmi", st); err != nil {
		t.Fatal(err)
	}
	c.Close(ctx)
	check("not verified", false)

	now := time.Now()
	st.FirstVerifiedAt = &now
	if err := maint.SaveState(ctx, "cmi", st); err != nil {
		t.Fatal(err)
	}
	check("verified and clean", true)

	if _, err := pool.Exec(ctx, "DELETE FROM channel_message_iatas WHERE message_id = (SELECT max(message_id) FROM channel_message_iatas) AND iata = 'YOW'"); err != nil {
		t.Fatal(err)
	}
	check("a missing pair among the newest", false)
}
