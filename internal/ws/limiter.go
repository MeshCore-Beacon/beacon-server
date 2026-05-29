package ws

import "sync"

// ipLimiter tracks the number of active WebSocket connections per IP address.
type ipLimiter struct {
	mu    sync.Mutex
	count map[string]int
	max   int
}

func newIPLimiter(max int) *ipLimiter {
	return &ipLimiter{
		count: make(map[string]int),
		max:   max,
	}
}

// acquire increments the connection count for the given IP and returns true
// if the connection is allowed. Returns false if the limit is already reached.
func (l *ipLimiter) acquire(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.count[ip] >= l.max {
		return false
	}
	l.count[ip]++
	return true
}

// release decrements the connection count for the given IP.
// The map entry is deleted when the count reaches zero to prevent unbounded growth.
func (l *ipLimiter) release(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.count[ip]--
	if l.count[ip] <= 0 {
		delete(l.count, ip)
	}
}
