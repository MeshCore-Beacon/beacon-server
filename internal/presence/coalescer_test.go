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
