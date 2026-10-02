// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"log/slog"
	"runtime/debug"
	"strings"
	"sync"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type messageQueue struct {
	mu              sync.Mutex
	lanes           []chan mqtt.Message
	bytes, maxBytes int
	dropped         uint64
	closed          bool
	done            chan struct{}
	cancel          context.CancelFunc
}

func newMessageQueue(workers, capacity, maxBytes int, handle func(context.Context, mqtt.Message)) *messageQueue {
	ctx, cancel := context.WithCancel(context.Background())
	q := &messageQueue{lanes: make([]chan mqtt.Message, workers), maxBytes: maxBytes, done: make(chan struct{}), cancel: cancel}
	var wg sync.WaitGroup
	for i := range q.lanes {
		lane := make(chan mqtt.Message, capacity)
		q.lanes[i] = lane
		wg.Go(func() {
			for m := range lane {
				if ctx.Err() != nil {
					return
				}
				handleSafely(ctx, m, handle)
				q.mu.Lock()
				q.bytes -= len(m.Topic()) + len(m.Payload())
				q.mu.Unlock()
			}
		})
	}
	go func() { wg.Wait(); close(q.done) }()
	return q
}

// handleSafely keeps one bad message from killing the lane, and with it the process.
func handleSafely(ctx context.Context, m mqtt.Message, handle func(context.Context, mqtt.Message)) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("ingest handler panicked", "component", "ingest", "topic", m.Topic(), "panic", r, "stack", string(debug.Stack()))
		}
	}()
	handle(ctx, m)
}

func (q *messageQueue) enqueue(m mqtt.Message) bool {
	// Keep status and packets from each observer in arrival order.
	parts := strings.SplitN(m.Topic(), "/", 4)
	key := m.Topic()
	if len(parts) == 4 {
		key = parts[2]
	}
	hash := uint32(2166136261)
	for i := range len(key) {
		c := key[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		hash = (hash ^ uint32(c)) * 16777619
	}
	size := len(m.Topic()) + len(m.Payload())
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	if size > q.maxBytes-q.bytes {
		q.dropped++
		return false
	}
	select {
	case q.lanes[int(hash%uint32(len(q.lanes)))] <- m:
		q.bytes += size
		return true
	default:
		q.dropped++
		return false
	}
}

func (q *messageQueue) takeDropped() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	n := q.dropped
	q.dropped = 0
	return n
}

func (q *messageQueue) close(ctx context.Context) error {
	q.mu.Lock()
	if !q.closed {
		q.closed = true
		for _, lane := range q.lanes {
			close(lane)
		}
	}
	q.mu.Unlock()
	select {
	case <-q.done:
		q.cancel()
		return nil
	case <-ctx.Done():
		q.cancel()
		<-q.done
		return ctx.Err()
	}
}
