// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
)

var actualRows = regexp.MustCompile(`actual time=[0-9.]+\.\.[0-9.]+ rows=([0-9]+) loops=([0-9]+)`)

// namedQuery cuts one sqlc query out of a query file, with its named arguments as $1, $2, ...
func namedQuery(t *testing.T, file, name string, args ...string) string {
	t.Helper()
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.SplitN(string(b), "-- name: "+name+" ", 2)
	if len(parts) != 2 {
		t.Fatalf("no query %s", name)
	}
	q := strings.SplitN(parts[1], "\n-- name: ", 2)[0]
	q = q[strings.Index(q, "\n")+1:]
	for i, a := range args {
		q = strings.ReplaceAll(q, "sqlc.arg("+a+")", "$"+strconv.Itoa(i+1))
	}
	return q
}

// Every retained reading of the repeater is from a former assignee: the latest-reading lookups must not walk them.
func TestOwnersLatestReadingBoundedPostgres(t *testing.T) {
	w := newReqWorld(t, "")
	o := newReqDevice(1)
	oid := pollObserver(t, w.ctx, w.s, o.pub)
	w.claim(o, 1)
	target := pollKey(0xA1)
	pollHearAdvert(t, w.ctx, w.pool, target, oid, w.now.Add(-time.Hour), 5)
	u := newUser(t, w.ctx, w.s, "alice")
	if _, err := w.reqs.Create(w.ctx, u.ID, w.node(0xA1), []byte("pw")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(w.ctx, `INSERT INTO repeater_readings (target_public_key, observer_public_key, sampled_at, received_at, method, tag, trusted)
		SELECT $1, $2, $3::timestamptz - g * interval '1 minute', $3, 'status', g, true FROM generate_series(1, 100000) g`,
		target, pollKey(99), w.now); err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(w.ctx, `ANALYZE repeater_readings; ANALYZE polling_assignments`); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		sql  string
		args []any
	}{
		{"owners", namedQuery(t, "queries/polling.sql", "PollingOwnerRows", "observer_public_key", "received_at"),
			[]any{o.pub, w.now.Add(-polling.ClaimTTL)}},
		{"admin", namedQuery(t, "queries/polling.sql", "PollingAdminTargets", "received_after", "row_limit"),
			[]any{w.now.Add(-polling.ClaimTTL), 50}},
	}
	for _, c := range cases {
		rows, err := w.pool.Query(w.ctx, "EXPLAIN (ANALYZE, BUFFERS) "+c.sql, c.args...)
		if err != nil {
			t.Fatal(c.name, err)
		}
		var plan []string
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			plan = append(plan, line)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			t.Fatal(c.name, err)
		}
		for _, line := range plan {
			m := actualRows.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			n, _ := strconv.Atoi(m[1])
			loops, _ := strconv.Atoi(m[2])
			if n*loops > 1000 {
				t.Fatalf("%s walks the old readings:\n%s", c.name, strings.Join(plan, "\n"))
			}
		}
	}
}

// The assignee has only untrusted readings and a former assignee has the trusted ones: the lookup must find
// nothing without reading or rejecting the history.
func TestAssigneeTrustedReadingBoundedPostgres(t *testing.T) {
	w := newReqWorld(t, "")
	o := newReqDevice(1)
	oid := pollObserver(t, w.ctx, w.s, o.pub)
	w.claim(o, 1)
	target := pollKey(0xA1)
	pollHearAdvert(t, w.ctx, w.pool, target, oid, w.now.Add(-time.Hour), 5)
	u := newUser(t, w.ctx, w.s, "alice")
	if _, err := w.reqs.Create(w.ctx, u.ID, w.node(0xA1), []byte("pw")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(w.ctx, `INSERT INTO repeater_readings (target_public_key, observer_public_key, sampled_at, received_at, method, tag, trusted)
		SELECT $1, CASE WHEN g % 2 = 0 THEN $2::bytea ELSE $3::bytea END, $4::timestamptz - g * interval '1 minute', $4, 'status', g, g % 2 = 1
		FROM generate_series(1, 100000) g`, target, o.pub, pollKey(99), w.now); err != nil {
		t.Fatal(err)
	}
	if _, err := w.pool.Exec(w.ctx, `ANALYZE repeater_readings; ANALYZE polling_assignments`); err != nil {
		t.Fatal(err)
	}
	rows, err := w.pool.Query(w.ctx, "EXPLAIN (ANALYZE, BUFFERS) "+namedQuery(t, "queries/telemetry_requests.sql", "ListRequestedAssigneeReadings"), target)
	if err != nil {
		t.Fatal(err)
	}
	var plan []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, line)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	removed := regexp.MustCompile(`Rows Removed by [A-Za-z ]+: ([0-9]+)`)
	for _, line := range plan {
		if m := actualRows.FindStringSubmatch(line); m != nil {
			n, _ := strconv.Atoi(m[1])
			loops, _ := strconv.Atoi(m[2])
			if n*loops > 1000 {
				t.Fatalf("the lookup returns the old readings:\n%s", strings.Join(plan, "\n"))
			}
		}
		if m := removed.FindStringSubmatch(line); m != nil {
			if n, _ := strconv.Atoi(m[1]); n > 1000 {
				t.Fatalf("the lookup rejects the old readings:\n%s", strings.Join(plan, "\n"))
			}
		}
	}
}
