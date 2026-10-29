// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package obscount

import (
	"maps"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ActivitySlot is the bucket the activity counts keep: the observer chart's 15 minutes.
const ActivitySlot = 15 * time.Minute

// ActivityKeep covers a 24-hour chart and the slot still filling.
const ActivityKeep = 25 * time.Hour

const slotMicros = int64(ActivitySlot / time.Microsecond)

// Bucket sums one observer's stored observations in one slot the way the activity query sums raw rows.
// Signal sums and SignalN cover rows whose RSSI and SNR are not both zero.
type Bucket struct {
	Observations int64
	AirtimeSum   float64
	AirtimeN     int64
	SNRSum       float64
	SNRMin       float32
	RSSISum      int64
	SignalN      int64
	Types        map[int16]int64
}

type activityKey struct {
	observer uuid.UUID
	slot     int64
}

// Activity holds per-observer, per-slot sums of stored observations. A nil *Activity counts nothing
// and is never live.
type Activity struct {
	mu     sync.Mutex
	slots  map[activityKey]*Bucket
	from   int64 // first slot none of whose observations predate the counter
	pruned int64 // slots before this were dropped
	now    func() time.Time
}

// NewActivity starts counting now.
func NewActivity(now func() time.Time) *Activity {
	first := now().Add(heardAtSlack).UnixMicro()
	from := first / slotMicros
	if first%slotMicros != 0 {
		from++
	}
	return &Activity{slots: map[activityKey]*Bucket{}, from: from, now: now}
}

// Add counts one newly stored observation with the values it was stored with.
func (a *Activity) Add(heardAt time.Time, observer uuid.UUID, payloadType, rssi int16, snr float32, airtimeMs *float32) {
	if a == nil {
		return
	}
	slot := heardAt.UnixMicro() / slotMicros // the stored timestamp keeps whole microseconds
	a.mu.Lock()
	defer a.mu.Unlock()
	if cut := a.now().Add(-ActivityKeep).UnixMicro() / slotMicros; cut > a.pruned {
		for k := range a.slots {
			if k.slot < cut {
				delete(a.slots, k)
			}
		}
		a.pruned = cut
	}
	if slot < a.pruned {
		return
	}
	k := activityKey{observer, slot}
	b := a.slots[k]
	if b == nil {
		b = &Bucket{Types: map[int16]int64{}}
		a.slots[k] = b
	}
	b.Observations++
	if airtimeMs != nil {
		b.AirtimeSum += float64(*airtimeMs)
		b.AirtimeN++
	}
	if rssi != 0 || snr != 0 {
		if b.SignalN == 0 || snr < b.SNRMin {
			b.SNRMin = snr
		}
		b.SNRSum += float64(snr)
		b.RSSISum += int64(rssi)
		b.SignalN++
	}
	b.Types[payloadType]++
}

// ActivityWindow is one observer's counted slots in [From, Until), keyed by slot start.
type ActivityWindow struct {
	From, Until time.Time
	Buckets     map[time.Time]Bucket
}

// Live reports whether the window covers any slot.
func (w ActivityWindow) Live() bool { return !w.From.IsZero() && w.From.Before(w.Until) }

// Window returns observer's counted slots between from and until. It covers only slots every one of whose
// observations was stored while counting, and stops at until's slot: that one can still hold rows heard after
// until, which only the raw rows can leave out.
func (a *Activity) Window(observer uuid.UUID, from, until time.Time) ActivityWindow {
	if a == nil {
		return ActivityWindow{}
	}
	first := from.UnixMicro() / slotMicros
	if from.UnixMicro()%slotMicros != 0 {
		first++
	}
	last := until.UnixMicro() / slotMicros
	a.mu.Lock()
	defer a.mu.Unlock()
	first = max(first, a.from, a.pruned)
	if first >= last {
		return ActivityWindow{}
	}
	w := ActivityWindow{From: slotStart(first), Until: slotStart(last), Buckets: map[time.Time]Bucket{}}
	for slot := first; slot < last; slot++ {
		if b := a.slots[activityKey{observer, slot}]; b != nil {
			c := *b
			c.Types = maps.Clone(b.Types)
			w.Buckets[slotStart(slot)] = c
		}
	}
	return w
}

func slotStart(slot int64) time.Time { return time.UnixMicro(slot * slotMicros).UTC() }
