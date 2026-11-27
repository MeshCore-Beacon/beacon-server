// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package users

import (
	"container/list"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	throttleMax    = 10
	throttleWindow = 15 * time.Minute
	sweepEvery     = time.Minute
	// throttleCap bounds the typed-name entries, which anyone can create by trying made-up names.
	throttleCap = 100000
	// fullRetry is what a new typed name is told to wait when the table is full of locked names.
	fullRetry = time.Minute

	namePrefix = "login:"
)

// loginKey is the key for a typed login name: the same whether it names a user, an email or nobody.
func loginKey(login string) string { return namePrefix + strings.ToLower(strings.TrimSpace(login)) }

// userKey is the key for a resolved account (its login attempts and its profile actions).
func userKey(id uuid.UUID) string { return "user:" + id.String() }

// throttle counts wrong passwords per key in memory (Beacon runs as a single writer). After throttleMax failures
// inside the window the key is locked for throttleWindow.
//
// User-id entries are bounded by the number of accounts, so they are never evicted or refused. Typed-name entries
// are capped: at the cap the oldest entry that is not locked is evicted, and only when every one is locked does a
// new name fail closed (fail reports full).
type throttle struct {
	mu        sync.Mutex
	users     map[string]*attempt
	names     map[string]*attempt
	order     *list.List // typed-name keys, oldest first; each attempt keeps its element
	cap       int
	nextSweep time.Time
}

type attempt struct {
	count       int
	windowStart time.Time
	lockedUntil time.Time
	elem        *list.Element // typed names only
}

func newThrottle() *throttle {
	return &throttle{users: map[string]*attempt{}, names: map[string]*attempt{}, order: list.New(), cap: throttleCap}
}

func isName(key string) bool { return strings.HasPrefix(key, namePrefix) }

func (a *attempt) expired(now time.Time) bool {
	return !now.Before(a.windowStart.Add(throttleWindow)) && !now.Before(a.lockedUntil)
}

func (t *throttle) remove(key string) {
	if isName(key) {
		if a := t.names[key]; a != nil {
			t.order.Remove(a.elem)
			delete(t.names, key)
		}
		return
	}
	delete(t.users, key)
}

// sweep drops expired entries, at most once a minute. The caller holds mu.
func (t *throttle) sweep(now time.Time) {
	if now.Before(t.nextSweep) {
		return
	}
	t.nextSweep = now.Add(sweepEvery)
	for k, a := range t.users {
		if a.expired(now) {
			delete(t.users, k)
		}
	}
	for k, a := range t.names {
		if a.expired(now) {
			t.remove(k)
		}
	}
}

func (t *throttle) entry(key string) *attempt {
	if isName(key) {
		return t.names[key]
	}
	return t.users[key]
}

// locked reports whether key may not be tried, and for how long still. A key with no entry is never locked.
func (t *throttle) locked(key string, now time.Time) (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sweep(now)
	if a := t.entry(key); a != nil && now.Before(a.lockedUntil) {
		return a.lockedUntil.Sub(now), true
	}
	return 0, false
}

// evictOldestUnlocked makes room for one typed name. It reports false when every entry is locked.
func (t *throttle) evictOldestUnlocked(now time.Time) bool {
	for e := t.order.Front(); e != nil; e = e.Next() {
		k := e.Value.(string)
		if !now.Before(t.names[k].lockedUntil) {
			t.remove(k)
			return true
		}
	}
	return false
}

// fail records a wrong password. locked says the key is now locked; full says a new typed name could not be
// recorded because every slot holds a lock, so the caller must refuse the attempt.
func (t *throttle) fail(key string, now time.Time) (locked, full bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sweep(now)
	a := t.entry(key)
	if a == nil {
		a = &attempt{}
		if isName(key) {
			if len(t.names) >= t.cap && !t.evictOldestUnlocked(now) {
				return false, true
			}
			a.elem = t.order.PushBack(key)
			t.names[key] = a
		} else {
			t.users[key] = a
		}
	}
	if a.count == 0 || !now.Before(a.windowStart.Add(throttleWindow)) {
		a.count, a.windowStart = 0, now
	}
	a.count++
	if a.count >= throttleMax {
		a.lockedUntil = now.Add(throttleWindow)
		return true, false
	}
	return false, false
}

func (t *throttle) clear(key string) {
	t.mu.Lock()
	t.remove(key)
	t.mu.Unlock()
}

func (t *throttle) size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.users) + len(t.names)
}
