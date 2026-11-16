// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package ws

import (
	"context"
	"github.com/coder/websocket"
	"time"
)

const maxSubscriptions = 16
const maxScopeValues = 256

// Only the connection's read pump owns this state. Incoming live events do not
// consume the control budget; idle pings and normal resubscriptions fit easily.
type controlBudget struct {
	at     time.Time
	tokens float64
	active map[string]struct{}
}

func newControlBudget() *controlBudget {
	return &controlBudget{at: time.Now(), tokens: 60, active: make(map[string]struct{})}
}

func (b *controlBudget) allow(m clientMessage, now time.Time) bool {
	b.tokens = min(60, b.tokens+max(0, now.Sub(b.at).Seconds()))
	b.at = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	if len(m.ID) > 128 || len(m.SubscriptionID) > 128 {
		return false
	}
	if m.Type != "subscribe" || m.Scope == nil {
		return true
	}
	if len(b.active) >= maxSubscriptions {
		return false
	}
	s := m.Scope
	count := len(s.PayloadTypes) + len(s.RouteTypes) + len(s.Events)
	for _, values := range [][]string{s.IATAs, s.RegionIDs, s.RegionSlugs, s.ChannelHashes, s.ObserverIDs} {
		count += len(values)
		for _, value := range values {
			if len(value) > 128 {
				return false
			}
		}
	}
	return count <= maxScopeValues
}

func writeFrame(ctx context.Context, conn *websocket.Conn, body []byte) error {
	writeCtx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, body)
}

func boundedIATAs(current, added []string, limit int) ([]string, bool) {
	seen := make(map[string]struct{})
	var result []string
	for _, values := range [][]string{current, added} {
		for _, value := range values {
			if _, ok := seen[value]; ok {
				continue
			}
			if len(result) >= limit {
				return nil, false
			}
			seen[value] = struct{}{}
			result = append(result, value)
		}
	}
	return result, true
}
