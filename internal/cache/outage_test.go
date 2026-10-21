// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package cache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestCacheOutageCoalescesAndCanceledWaiterDoesNotFetch(t *testing.T) {
	c, _ := newTestClient(t)
	_ = c.Close()
	var calls atomic.Int32
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	fetch := func(context.Context) (int, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return 7, nil
	}
	go func() { _, err := getOrSet(context.Background(), c, "outage", time.Minute, fetch); done <- err }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	waiter := make(chan error, 1)
	go func() { _, err := getOrSet(ctx, c, "outage", time.Minute, fetch); waiter <- err }()
	var err error
	select {
	case err = <-waiter:
	case <-time.After(time.Second):
		err = errors.New("canceled cache waiter remained blocked")
	}
	close(release)
	if first := <-done; first != nil {
		t.Fatal(first)
	}
	if !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
		t.Fatalf("waiter error=%v, database fetches=%d", err, calls.Load())
	}
}
