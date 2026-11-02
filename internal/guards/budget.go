// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package guards

import "time"

// sweepEvery is how many debits pass between sweeps of idle budgets.
const sweepEvery = 1024

// bucket is a client's database-time budget in seconds, refilled lazily at one
// second per second up to BudgetBurst.
type bucket struct {
	tokens float64
	at     time.Time
}

func (b *bucket) refill(now time.Time) {
	b.tokens = min(BudgetBurst.Seconds(), b.tokens+now.Sub(b.at).Seconds())
	b.at = now
}

// debit charges d to key's budget and reports whether the budget went below
// zero. The budget only observes: it never refuses.
func (g *Guard) debit(key string, d time.Duration, now time.Time) bool {
	g.bmu.Lock()
	defer g.bmu.Unlock()
	b := g.budgets[key]
	if b == nil {
		b = &bucket{tokens: BudgetBurst.Seconds(), at: now}
		g.budgets[key] = b
	}
	b.refill(now)
	b.tokens -= d.Seconds()
	over := b.tokens < 0
	g.debits++
	if g.debits%sweepEvery == 0 {
		for k, other := range g.budgets {
			other.refill(now)
			if other.tokens >= BudgetBurst.Seconds() {
				delete(g.budgets, k)
			}
		}
	}
	return over
}
