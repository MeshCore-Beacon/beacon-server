// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

type blockedRevisionReader struct {
	api.Reader
	calls atomic.Int64
	read  func(context.Context) (int64, error)
}

func (r *blockedRevisionReader) AnalyticsRevision(ctx context.Context) (int64, error) {
	r.calls.Add(1)
	return r.read(ctx)
}

func TestRevisionWaiterCanCancel(t *testing.T) {
	started, release, ownerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	r := &blockedRevisionReader{read: func(context.Context) (int64, error) {
		close(started)
		<-release
		return 7, nil
	}}
	var memo revisionMemo
	go func() { defer close(ownerDone); memo.get(context.Background(), r) }()
	<-started
	defer func() { close(release); <-ownerDone }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan bool, 1)
	go func() { _, ok := memo.get(ctx, r); done <- ok }()
	select {
	case ok := <-done:
		if ok || ctx.Err() != context.DeadlineExceeded {
			t.Fatalf("waiter returned ok=%v, context error=%v", ok, ctx.Err())
		}
	case <-time.After(time.Second):
		t.Error("canceled revision waiter stayed blocked on another request's database read")
	}
	if n := r.calls.Load(); n != 1 {
		t.Errorf("revision reads = %d, want only the owner", n)
	}
}

func TestRevisionConcurrentRefreshIsShared(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	r := &blockedRevisionReader{read: func(context.Context) (int64, error) {
		close(started)
		<-release
		return 7, nil
	}}
	var memo revisionMemo
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if rev, ok := memo.get(context.Background(), r); !ok || rev != 7 {
				t.Errorf("revision = %d, ok = %v", rev, ok)
			}
		})
	}
	<-started
	close(release)
	wg.Wait()
	if n := r.calls.Load(); n != 1 {
		t.Errorf("concurrent revision reads = %d, want 1", n)
	}
}

func TestRevisionWaiterRetriesAfterOwnerCancellation(t *testing.T) {
	started := make(chan struct{})
	r := &blockedRevisionReader{}
	r.read = func(ctx context.Context) (int64, error) {
		if r.calls.Load() == 1 {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		}
		return 8, nil
	}
	var memo revisionMemo
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ownerDone := make(chan bool, 1)
	go func() { _, ok := memo.get(ctx, r); ownerDone <- ok }()
	<-started
	waiterDone := make(chan bool, 1)
	go func() { rev, ok := memo.get(context.Background(), r); waiterDone <- ok && rev == 8 }()
	cancel()
	if <-ownerDone {
		t.Error("canceled owner's failed revision read was cached")
	}
	select {
	case ok := <-waiterDone:
		if !ok {
			t.Error("waiter inherited the owner's canceled revision read")
		}
	case <-time.After(time.Second):
		t.Fatal("waiter did not retry after owner cancellation")
	}
	if n := r.calls.Load(); n != 2 {
		t.Errorf("revision reads = %d, want failed owner plus successful retry", n)
	}
}
