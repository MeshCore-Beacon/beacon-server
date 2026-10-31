// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package presence

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
)

type retentionStore struct {
	Store
	id    uuid.UUID
	touch func(context.Context, []uuid.UUID, []time.Time, []int32, []string, []time.Time) error
	prune func(context.Context, time.Time) ([]uuid.UUID, error)
}

func (s *retentionStore) UpsertObserver(context.Context, []byte, string) (uuid.UUID, string, error) {
	return s.id, "fixture", nil
}
func (s *retentionStore) TouchObservers(ctx context.Context, ids []uuid.UUID, seen []time.Time, counts []int32, iatas []string, iataAts []time.Time) error {
	return s.touch(ctx, ids, seen, counts, iatas, iataAts)
}
func (s *retentionStore) DeleteOldObservers(ctx context.Context, cutoff time.Time) ([]uuid.UUID, error) {
	return s.prune(ctx, cutoff)
}

func TestObserverRetentionAbortOnFlushFailure(t *testing.T) {
	for _, cause := range []error{errors.New("flush unavailable"), context.Canceled} {
		t.Run(cause.Error(), func(t *testing.T) {
			store := &retentionStore{id: uuid.New()}
			store.touch = func(context.Context, []uuid.UUID, []time.Time, []int32, []string, []time.Time) error { return cause }
			store.prune = func(context.Context, time.Time) ([]uuid.UUID, error) {
				t.Error("deletion attempted after failed preparation")
				return nil, nil
			}
			c := New(store, time.Second, time.Second)
			if _, _, err := c.UpsertObserver(context.Background(), []byte("fixture"), "YVR"); err != nil {
				t.Fatal(err)
			}
			if _, err := c.DeleteOldObservers(context.Background(), time.Now()); !errors.Is(err, cause) {
				t.Fatalf("got %v, want %v", err, cause)
			}
		})
	}
}

func TestObserverRetentionPreservesActivityAfterFlush(t *testing.T) {
	for _, inFlight := range []bool{false, true} {
		name := "failed flush"
		if inFlight {
			name = "in-flight flush"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			store := &retentionStore{id: uuid.New()}
			c := New(store, time.Second, time.Second)
			c.now = func() time.Time { return now }
			for range 2 {
				if _, _, err := c.UpsertObserver(ctx, []byte("fixture"), "YVR"); err != nil {
					t.Fatal(err)
				}
			}
			started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			store.touch = func(_ context.Context, ids []uuid.UUID, seen []time.Time, counts []int32, _ []string, _ []time.Time) error {
				if calls.Add(1) == 1 {
					close(started)
					<-release
					return errors.New("earlier flush failed")
				}
				if len(ids) != 1 || ids[0] != store.id || !seen[0].Equal(now) || counts[0] != 0 {
					t.Errorf("cached activity was lost or counted twice: %v, %v, %v", ids, seen, counts)
				}
				return nil
			}
			store.prune = func(context.Context, time.Time) ([]uuid.UUID, error) {
				if calls.Load() != 2 {
					t.Error("deletion preceded the freshness write")
				}
				return nil, nil
			}
			go func() { c.Flush(ctx); close(done) }()
			<-started
			if !inFlight {
				close(release)
				<-done
			}
			_, err := c.DeleteOldObservers(ctx, now.Add(-time.Hour))
			if inFlight {
				close(release)
				<-done
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func (s *retentionStore) UpdateObserverStatus(context.Context, ingest.UpdateObserverStatusParams) (uuid.UUID, error) {
	return s.id, nil
}

func TestObserverLastIATAFlush(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := &retentionStore{id: uuid.New()}
	var gotIATAs []string
	var gotAts []time.Time
	store.touch = func(_ context.Context, _ []uuid.UUID, _ []time.Time, _ []int32, iatas []string, ats []time.Time) error {
		gotIATAs, gotAts = iatas, ats
		return nil
	}
	c := New(store, time.Second, time.Second)
	c.now = func() time.Time { return now }
	pk := []byte("fixture")
	upsert := func(iata string) {
		t.Helper()
		if _, _, err := c.UpsertObserver(ctx, pk, iata); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
	}

	upsert("YVR") // write-through
	upsert("YYZ")
	heard := now.Add(-time.Second)
	upsert("") // status/neighbors path must not clear it or move its timestamp
	c.Flush(ctx)
	if len(gotIATAs) != 1 || gotIATAs[0] != "YYZ" || !gotAts[0].Equal(heard) {
		t.Fatalf("flushed %v at %v, want YYZ at %s", gotIATAs, gotAts, heard)
	}
}

// shutdownStore blocks the first observer flush until release closes, and reports a flush that starts once the pool is
// closed.
type shutdownStore struct {
	retentionStore
	release chan struct{}
	closed  atomic.Bool
	flushes atomic.Int32
	counts  atomic.Int32
	t       *testing.T
}

func (s *shutdownStore) TouchObservers(ctx context.Context, ids []uuid.UUID, seen []time.Time, counts []int32, iatas []string, iataAts []time.Time) error {
	if s.closed.Load() {
		s.t.Error("observer flush after the pool closed")
	}
	if s.flushes.Add(1) == 1 {
		<-s.release
	}
	for _, n := range counts {
		s.counts.Add(n)
	}
	if s.closed.Load() {
		s.t.Error("observer flush still running when the pool closed")
	}
	return nil
}

// On shutdown, finish returns only after Run's final flush has ended, and it flushes the bumps written after that
// flush took its snapshot, so closing the pool right after finish drops nothing.
func TestCoalescerShutdownFlushesBeforePoolClose(t *testing.T) {
	store := &shutdownStore{retentionStore: retentionStore{id: uuid.New()}, release: make(chan struct{}), t: t}
	c := New(store, time.Hour, time.Hour)
	bump := func() {
		t.Helper()
		if _, _, err := c.UpsertObserver(context.Background(), []byte("fixture"), "YVR"); err != nil {
			t.Fatal(err)
		}
	}
	bump() // identity
	bump() // one bump for Run's final flush
	ctx, cancel := context.WithCancel(context.Background())
	finish := c.Start(ctx)
	cancel()
	// Run's final flush has taken its snapshot and blocks in TouchObservers.
	deadline := time.Now().Add(5 * time.Second)
	for store.flushes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if store.flushes.Load() == 0 {
		t.Fatal("Run's final flush never started")
	}
	bump() // a writer that stopped late

	finished := make(chan struct{})
	go func() {
		finish(context.Background())
		close(finished)
	}()
	select {
	case <-finished:
		t.Fatal("finish returned while Run's final flush was still running")
	case <-time.After(100 * time.Millisecond):
	}
	close(store.release)
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("finish did not return")
	}
	store.closed.Store(true) // main's deferred pools.Close
	time.Sleep(50 * time.Millisecond)
	if n := store.counts.Load(); n != 2 {
		t.Errorf("flushed %d observations, want 2 (Run's final flush and the late one)", n)
	}
}

// A ticker flush in flight when shutdown starts finishes on its own context: its rows are written, not dropped.
func TestCoalescerTickerFlushSurvivesShutdown(t *testing.T) {
	store := &shutdownStore{retentionStore: retentionStore{id: uuid.New()}, release: make(chan struct{}), t: t}
	var ctxErr atomic.Value
	c := New(&ctxStore{shutdownStore: store, ctxErr: &ctxErr}, 20*time.Millisecond, time.Hour)
	for range 2 {
		if _, _, err := c.UpsertObserver(context.Background(), []byte("fixture"), "YVR"); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	finish := c.Start(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for store.flushes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if store.flushes.Load() == 0 {
		t.Fatal("no ticker flush started")
	}
	cancel() // shutdown while the ticker flush is blocked
	time.Sleep(20 * time.Millisecond)
	close(store.release)
	finish(context.Background())
	if err, _ := ctxErr.Load().(error); err != nil {
		t.Errorf("ticker flush saw a cancelled context: %v", err)
	}
	if n := store.counts.Load(); n != 1 {
		t.Errorf("flushed %d observations, want 1", n)
	}
}

// ctxStore records whether the first observer flush's context was cancelled by the time it wrote.
type ctxStore struct {
	*shutdownStore
	ctxErr *atomic.Value
}

func (s *ctxStore) TouchObservers(ctx context.Context, ids []uuid.UUID, seen []time.Time, counts []int32, iatas []string, iataAts []time.Time) error {
	err := s.shutdownStore.TouchObservers(ctx, ids, seen, counts, iatas, iataAts)
	if ctx.Err() != nil {
		s.ctxErr.CompareAndSwap(nil, ctx.Err())
	}
	return err
}

// The observer cleanup takes the dirty observers out of the coalescer, so a shutdown mid-cleanup must not cancel their
// flush: it runs on its own context and writes them.
func TestObserverCleanupFlushSurvivesShutdown(t *testing.T) {
	var flushErr atomic.Value
	var flushed atomic.Int32
	store := &retentionStore{id: uuid.New()}
	store.touch = func(ctx context.Context, _ []uuid.UUID, _ []time.Time, counts []int32, _ []string, _ []time.Time) error {
		if ctx.Err() != nil {
			flushErr.Store(ctx.Err())
			return ctx.Err()
		}
		for _, n := range counts {
			flushed.Add(n)
		}
		return nil
	}
	store.prune = func(ctx context.Context, _ time.Time) ([]uuid.UUID, error) { return nil, ctx.Err() }
	c := New(store, time.Hour, time.Hour)
	for range 2 {
		if _, _, err := c.UpsertObserver(context.Background(), []byte("fixture"), "YVR"); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _ = c.DeleteOldObservers(ctx, time.Now())
	if err, _ := flushErr.Load().(error); err != nil {
		t.Errorf("cleanup flush ran on a cancelled context: %v", err)
	}
	if n := flushed.Load(); n != 1 {
		t.Errorf("cleanup flushed %d observations, want 1", n)
	}
}
