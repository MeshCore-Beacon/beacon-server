// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"sync"
	"testing"
	"time"
)

func TestWaitAtMost(t *testing.T) {
	var wg sync.WaitGroup
	wg.Go(func() { time.Sleep(10 * time.Millisecond) })
	start := time.Now()
	waitAtMost(&wg, time.Second, "quick job")
	if d := time.Since(start); d < 10*time.Millisecond || d > 500*time.Millisecond {
		t.Errorf("waited %s for a 10 ms job", d)
	}

	stuck := make(chan struct{})
	defer close(stuck)
	var slow sync.WaitGroup
	slow.Go(func() { <-stuck })
	start = time.Now()
	waitAtMost(&slow, 50*time.Millisecond, "stuck job")
	if d := time.Since(start); d < 50*time.Millisecond || d > time.Second {
		t.Errorf("waited %s for a stuck job, want about 50 ms", d)
	}
}
