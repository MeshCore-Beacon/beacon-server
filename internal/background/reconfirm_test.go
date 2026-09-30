// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
)

type reconfirmStore struct {
	run       func(context.Context, int32, time.Time) (int64, error)
	ambCalls  int
	neighbors bool
}

func (*reconfirmStore) DeleteOldRoutes(context.Context, time.Time, int64, time.Time) error {
	return nil
}

func (s *reconfirmStore) AmbiguousPrefixes(context.Context) (db.AmbiguousPrefixes, error) {
	s.ambCalls++
	return db.AmbiguousPrefixes{}, nil
}

func (s *reconfirmStore) ReconfirmRoutes(ctx context.Context, n int32, before time.Time, _ db.AmbiguousPrefixes) (int64, error) {
	return s.run(ctx, n, before)
}

func (s *reconfirmStore) ReconfirmNeighbors(context.Context) error {
	s.neighbors = true
	return nil
}

func TestReconfirmCoverage(t *testing.T) {
	for _, tc := range []struct {
		name                string
		available, wantLeft int64
	}{
		{"empty", 0, 0},
		{"partial last batch", 1501, 0},
		{"full run budget", 750010, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left := tc.available
			var cutoff time.Time
			s := &reconfirmStore{run: func(ctx context.Context, n int32, before time.Time) (int64, error) {
				if n <= 0 || n > 1000 {
					t.Fatalf("transaction requested %d routes", n)
				}
				if cutoff.IsZero() {
					cutoff = before
				} else if !cutoff.Equal(before) {
					t.Fatal("cutoff advanced, allowing completed routes to be revisited")
				}
				checked := min(int64(n), left)
				left -= checked
				return checked, ctx.Err()
			}}
			if err := ReconfirmTask(s, time.Hour, time.Minute, 3, time.Hour).Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			if left != tc.wantLeft || !s.neighbors {
				t.Fatalf("remaining %d, neighbor cleanup %t", left, s.neighbors)
			}
			if s.ambCalls != 1 {
				t.Fatalf("ambiguity computed %d times, want once per run", s.ambCalls)
			}
		})
	}
}

func TestReconfirmStopsOnFailure(t *testing.T) {
	want := errors.New("database unavailable")
	calls := 0
	s := &reconfirmStore{run: func(context.Context, int32, time.Time) (int64, error) {
		calls++
		if calls == 1 {
			return 1000, nil
		}
		return 0, want
	}}
	err := ReconfirmTask(s, time.Hour, time.Minute, 3, time.Hour).Run(context.Background())
	if !errors.Is(err, want) || calls != 2 || s.neighbors {
		t.Fatalf("failure was not propagated: error %v, calls %d, neighbors %t", err, calls, s.neighbors)
	}
}

func TestReconfirmBatchDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &reconfirmStore{run: func(ctx context.Context, _ int32, _ time.Time) (int64, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		}}
		start := time.Now()
		err := ReconfirmTask(s, time.Hour, time.Minute, 3, time.Hour).Run(context.Background())
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 5*time.Second || s.neighbors {
			t.Fatalf("batch exceeded its deadline: error %v, duration %s, neighbors %t", err, time.Since(start), s.neighbors)
		}
	})
}
