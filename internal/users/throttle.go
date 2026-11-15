// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package users

import (
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	throttleMax    = 10
	throttleWindow = 15 * time.Minute
	sweepEvery     = time.Minute
	// throttleCap bounds the table so made-up login names cannot grow it without limit.
	throttleCap = 20000
)

// loginKey is the key for a typed login name: the same whether it names a user, an email or nobody.
func loginKey(login string) string { return "login:" + strings.ToLower(strings.TrimSpace(login)) }

// userKey is the key for actions of a signed-in user (change password, change email, delete).
func userKey(id uuid.UUID) string { return "user:" + id.String() }

// throttle counts wrong passwords per key (see loginKey, userKey) in memory (Beacon runs as a single writer). After throttleMax failures
// inside the window the key is locked for throttleWindow. Expired entries are swept so the map stays small.
type throttle struct {
	mu        sync.Mutex
	m         map[string]*attempt
	nextSweep time.Time
}

type attempt struct {
	count       int
	windowStart time.Time
	lockedUntil time.Time
}

func newThrottle() *throttle { return &throttle{m: map[string]*attempt{}} }

func (a *attempt) expired(now time.Time) bool {
	return !now.Before(a.windowStart.Add(throttleWindow)) && !now.Before(a.lockedUntil)
}

// sweep drops expired entries, at most once a minute. The caller holds mu.
func (t *throttle) sweep(now time.Time) {
	if now.Before(t.nextSweep) {
		return
	}
	t.nextSweep = now.Add(sweepEvery)
	for id, a := range t.m {
		if a.expired(now) {
			delete(t.m, id)
		}
	}
}

// locked reports whether id may not be tried, and for how long still.
func (t *throttle) locked(id string, now time.Time) (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sweep(now)
	if a := t.m[id]; a != nil && now.Before(a.lockedUntil) {
		return a.lockedUntil.Sub(now), true
	}
	return 0, false
}

// fail records a wrong password and reports whether it locked the user.
func (t *throttle) fail(id string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sweep(now)
	a := t.m[id]
	if a == nil {
		if len(t.m) >= throttleCap {
			t.nextSweep = time.Time{}
			t.sweep(now)
			// Still full: do not record the new key, so a flood cannot evict a lock that is already set.
			if len(t.m) >= throttleCap {
				return false
			}
		}
		a = &attempt{}
		t.m[id] = a
	}
	if a.count == 0 || !now.Before(a.windowStart.Add(throttleWindow)) {
		a.count, a.windowStart = 0, now
	}
	a.count++
	if a.count >= throttleMax {
		a.lockedUntil = now.Add(throttleWindow)
		return true
	}
	return false
}

func (t *throttle) clear(id string) {
	t.mu.Lock()
	delete(t.m, id)
	t.mu.Unlock()
}

func (t *throttle) size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.m)
}
