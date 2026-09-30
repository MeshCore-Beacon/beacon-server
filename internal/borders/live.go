// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package borders

import "sync/atomic"

// Live lets boundary refreshes replace the classifier under concurrent readers.
// A nil Live disables classification; an empty one reports unknown until stored.
type Live struct{ current atomic.Pointer[Local] }

func NewLive(local *Local) *Live {
	live := &Live{}
	live.current.Store(local)
	return live
}

func (l *Live) Store(local *Local) { l.current.Store(local) }

// Ready reports whether any boundary is loaded.
func (l *Live) Ready() bool { return l != nil && l.current.Load() != nil }

func (l *Live) PossiblyForeign(role int16, lat, lng *float64) *bool {
	if l == nil {
		return nil
	}
	return l.current.Load().PossiblyForeign(role, lat, lng)
}
