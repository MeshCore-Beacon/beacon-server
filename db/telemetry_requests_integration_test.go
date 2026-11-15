// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/google/uuid"
)

func reqKey(b byte) []byte { return atlasKey(b) }

func newReq(user uuid.UUID, at time.Time, password bool) polling.Request {
	r := polling.Request{ID: uuid.New(), UserID: user, CreatedAt: at, EnabledAt: at}
	if password {
		r.HasPassword, r.PasswordCT, r.PasswordNonce = true, []byte("ct-"+r.ID.String()), []byte("nonce-123456")
	}
	return r
}

func TestTelemetryRequestsPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	alice, bob := newUser(t, ctx, s, "alice"), newUser(t, ctx, s, "bob")
	target := reqKey(1)
	now := time.Now().UTC().Truncate(time.Microsecond)

	create := func(u uuid.UUID, k []byte, at time.Time, pw bool, limit int) (polling.Request, error) {
		r := newReq(u, at, pw)
		return r, s.MutateRequestTarget(ctx, k, u, func(st *polling.TargetState) error {
			if st.UserRequests >= limit {
				return polling.ErrRequestLimit
			}
			return st.Create(r, at)
		})
	}
	a, err := create(alice.ID, target, now, true, 3)
	if err != nil {
		t.Fatal(err)
	}
	b, err := create(bob.ID, target, now.Add(time.Minute), true, 3)
	if err != nil {
		t.Fatal(err)
	}

	// The oldest request's password is in use; ver rose once.
	var active uuid.UUID
	var ver int64
	if err := pool.QueryRow(ctx, `SELECT active_request_id, ver FROM polling_targets WHERE target_public_key = $1`, target).Scan(&active, &ver); err != nil {
		t.Fatal(err)
	}
	if active != a.ID || ver != 1 {
		t.Fatalf("active=%v ver=%d", active, ver)
	}

	// One request per user and repeater, enforced in the state and in the table.
	if _, err := create(alice.ID, target, now, false, 3); !errors.Is(err, polling.ErrAlreadyRequested) {
		t.Errorf("duplicate: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO telemetry_requests (id, user_id, target_public_key, status, created_at, updated_at, enabled_at)
		VALUES (gen_random_uuid(), $1, $2, 'waiting', now(), now(), now())`, alice.ID, target); err == nil {
		t.Error("table allowed a second request of the same user for the same repeater")
	}

	// The limit counts across repeaters.
	for i, k := range []byte{2, 3} {
		if _, err := create(alice.ID, reqKey(k), now.Add(time.Duration(k)*time.Minute), false, 3); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	if _, err := create(alice.ID, reqKey(4), now, false, 3); !errors.Is(err, polling.ErrRequestLimit) {
		t.Errorf("limit: %v", err)
	}
	var left int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM polling_targets WHERE target_public_key = $1`, reqKey(4)).Scan(&left); err != nil || left != 0 {
		t.Errorf("a refused request left a target row: %d %v", left, err)
	}

	// Password replacement: sealed bytes change, ver rises, nothing else of another user's request moves.
	if err := s.MutateRequestTarget(ctx, target, alice.ID, func(st *polling.TargetState) error {
		return st.SetPassword(a.ID, []byte("new-ct"), []byte("new-nonce-1"), now.Add(time.Hour))
	}); err != nil {
		t.Fatal(err)
	}
	var ct []byte
	if err := pool.QueryRow(ctx, `SELECT password_ct FROM telemetry_requests WHERE id = $1`, a.ID).Scan(&ct); err != nil || string(ct) != "new-ct" {
		t.Fatalf("ct %q %v", ct, err)
	}
	if err := pool.QueryRow(ctx, `SELECT ver FROM polling_targets WHERE target_public_key = $1`, target).Scan(&ver); err != nil || ver != 2 {
		t.Fatalf("ver after replacement: %d %v", ver, err)
	}

	// The listing carries no password and resolves the node, poller and reading.
	var nodeID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO nodes (public_key, node_type, name) VALUES ($1, 2, 'Hilltop') RETURNING id`, target).Scan(&nodeID); err != nil {
		t.Fatal(err)
	}
	rows, err := s.UserTelemetryRequests(ctx, alice.ID)
	if err != nil || len(rows) != 3 {
		t.Fatalf("list: %d %v", len(rows), err)
	}
	first := rows[0]
	if first.ID != a.ID || first.NodeID == nil || *first.NodeID != nodeID || first.NodeName != "Hilltop" || !first.HasPassword ||
		first.PollerID != nil || first.LatestAt != nil || first.Status != polling.StatusWaiting {
		t.Fatalf("first: %+v", first)
	}
	if rows[1].NodeID != nil {
		t.Errorf("unknown node resolved: %+v", rows[1])
	}
	if kt, err := s.UserRequestTarget(ctx, alice.ID, a.ID); err != nil || string(kt) != string(target) {
		t.Errorf("target: %x %v", kt, err)
	}
	if _, err := s.UserRequestTarget(ctx, bob.ID, a.ID); !errors.Is(err, polling.ErrRequestNotFound) {
		t.Errorf("another user's request: %v", err)
	}
	n, err := s.RequestNode(ctx, nodeID)
	if err != nil || n.NodeType != 2 || n.Name != "Hilltop" || string(n.PublicKey) != string(target) {
		t.Errorf("node: %+v %v", n, err)
	}
	if _, err := s.RequestNode(ctx, uuid.New()); !errors.Is(err, polling.ErrRequestNotFound) {
		t.Errorf("unknown node: %v", err)
	}

	// Deleting the account removes its requests and passwords; bob's request takes over the repeater and ver rises.
	if err := s.DeleteUser(ctx, alice.ID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM telemetry_requests WHERE user_id = $1`, alice.ID).Scan(&left); err != nil || left != 0 {
		t.Fatalf("alice's requests left: %d %v", left, err)
	}
	if err := pool.QueryRow(ctx, `SELECT active_request_id, ver FROM polling_targets WHERE target_public_key = $1`, target).Scan(&active, &ver); err != nil {
		t.Fatal(err)
	}
	if active != b.ID || ver != 3 {
		t.Fatalf("after delete: active=%v want %v ver=%d", active, b.ID, ver)
	}
	// And with the last request gone the repeater is not requested, ver kept.
	if err := s.DeleteUser(ctx, bob.ID); err != nil {
		t.Fatal(err)
	}
	var activeNull *uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT active_request_id, ver FROM polling_targets WHERE target_public_key = $1`, target).Scan(&activeNull, &ver); err != nil {
		t.Fatal(err)
	}
	if activeNull != nil || ver != 4 {
		t.Fatalf("last request: active=%v ver=%d", activeNull, ver)
	}
	// A user that is gone cannot create one.
	if _, err := create(alice.ID, target, now, false, 3); !errors.Is(err, polling.ErrRequestNotFound) {
		t.Errorf("deleted user: %v", err)
	}
}

func TestTelemetryRequestsLimitRacePostgres(t *testing.T) {
	ctx, _, s := userStore(t)
	u := newUser(t, ctx, s, "racer")
	var ok, refused atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			r := newReq(u.ID, time.Now(), false)
			err := s.MutateRequestTarget(context.Background(), reqKey(byte(i+1)), u.ID, func(st *polling.TargetState) error {
				if st.UserRequests >= 3 {
					return polling.ErrRequestLimit
				}
				return st.Create(r, time.Now())
			})
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, polling.ErrRequestLimit):
				refused.Add(1)
			default:
				t.Error(err)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if ok.Load() != 3 || refused.Load() != 7 {
		t.Fatalf("ok=%d refused=%d", ok.Load(), refused.Load())
	}
}
