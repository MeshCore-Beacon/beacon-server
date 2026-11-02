// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ws

import "time"

// sendBudget is a byte token bucket for one connection's live events. It may go into debt by one
// message, so an event bigger than the burst still goes out once the bucket has refilled.
type sendBudget struct {
	rate, burst float64
	tokens      float64
	last        time.Time
}

// newSendBudget returns nil, meaning no limit, when bytesPerSecond is not positive.
func newSendBudget(bytesPerSecond, burstBytes int, now time.Time) *sendBudget {
	if bytesPerSecond <= 0 {
		return nil
	}
	burst := bytesPerSecond
	if burstBytes > 0 {
		burst = burstBytes
	}
	return &sendBudget{rate: float64(bytesPerSecond), burst: float64(burst), tokens: float64(burst), last: now}
}

// allow reports whether an n-byte message may go out now, and charges it if so.
func (b *sendBudget) allow(n int, now time.Time) bool {
	if b == nil {
		return true
	}
	b.tokens = min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	b.last = now
	if b.tokens <= 0 {
		return false
	}
	b.tokens -= float64(n)
	return true
}
