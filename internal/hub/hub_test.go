// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package hub

import (
	"encoding/json"
	"testing"
	"time"
)

func TestScopeMatches_EmptyScope(t *testing.T) {
	// empty scope matches everything — no filters means no restrictions
	s := Scope{}
	e := Event{Type: EventPacketObservation, IATA: "YVR", PayloadType: 4}
	if !scopeMatches(s, e) {
		t.Error("empty scope should match all events")
	}
}

func TestScopeMatches_EventFilter(t *testing.T) {
	s := Scope{Events: []EventType{EventNodeUpdate}}
	if !scopeMatches(s, Event{Type: EventNodeUpdate, IATA: "YVR"}) {
		t.Error("expected nodeUpdate to match")
	}
	if scopeMatches(s, Event{Type: EventPacketObservation, IATA: "YVR"}) {
		t.Error("expected packetObservation not to match")
	}
}

func TestScopeMatches_IATAFilter(t *testing.T) {
	s := Scope{Events: []EventType{EventPacketObservation}, IATAs: []string{"YVR", "YYJ"}}
	if !scopeMatches(s, Event{Type: EventPacketObservation, IATA: "YVR"}) {
		t.Error("expected YVR to match")
	}
	if !scopeMatches(s, Event{Type: EventPacketObservation, IATA: "YYJ"}) {
		t.Error("expected YYJ to match")
	}
	if scopeMatches(s, Event{Type: EventPacketObservation, IATA: "YYC"}) {
		t.Error("expected YYC not to match")
	}
}

func TestScopeMatches_PayloadTypeFilter(t *testing.T) {
	s := Scope{Events: []EventType{EventPacketObservation}, PayloadTypes: []uint8{4}}
	if !scopeMatches(s, Event{Type: EventPacketObservation, PayloadType: 4}) {
		t.Error("expected payload type 4 to match")
	}
	if scopeMatches(s, Event{Type: EventPacketObservation, PayloadType: 5}) {
		t.Error("expected payload type 5 not to match")
	}
}

func TestScopeMatches_ChannelHashFilter(t *testing.T) {
	s := Scope{Events: []EventType{EventChannelMessage}, ChannelHashes: []string{"ab"}}
	if !scopeMatches(s, Event{Type: EventChannelMessage, ChannelHash: "ab"}) {
		t.Error("expected channel hash ab to match")
	}
	if scopeMatches(s, Event{Type: EventChannelMessage, ChannelHash: "cd"}) {
		t.Error("expected channel hash cd not to match")
	}
}

func TestScopeMatches_ChannelHashesOnlyAppliesToChannelMessage(t *testing.T) {
	s := Scope{
		Events:        []EventType{EventPacketObservation, EventChannelMessage},
		ChannelHashes: []string{"11"},
	}
	// packetObservation has no channel hash — must still match despite the
	// channelHashes filter, since that filter only applies to channelMessage.
	if !scopeMatches(s, Event{Type: EventPacketObservation, ChannelHash: ""}) {
		t.Error("expected packetObservation to match even with channelHashes set")
	}
	if !scopeMatches(s, Event{Type: EventChannelMessage, ChannelHash: "11"}) {
		t.Error("expected channelMessage with matching hash to match")
	}
	if scopeMatches(s, Event{Type: EventChannelMessage, ChannelHash: "ff"}) {
		t.Error("expected channelMessage with non-matching hash to be filtered")
	}
}

func TestScopeMatches_AllFiltersPass(t *testing.T) {
	s := Scope{
		Events:       []EventType{EventPacketObservation},
		IATAs:        []string{"YVR"},
		PayloadTypes: []uint8{4},
	}
	if !scopeMatches(s, Event{Type: EventPacketObservation, IATA: "YVR", PayloadType: 4}) {
		t.Error("expected all-matching event to pass")
	}
}

func TestScopeMatches_OneFilterFails(t *testing.T) {
	s := Scope{
		Events:       []EventType{EventPacketObservation},
		IATAs:        []string{"YVR"},
		PayloadTypes: []uint8{4},
	}
	if scopeMatches(s, Event{Type: EventPacketObservation, IATA: "YYC", PayloadType: 4}) {
		t.Error("expected wrong IATA to fail")
	}
	if scopeMatches(s, Event{Type: EventPacketObservation, IATA: "YVR", PayloadType: 5}) {
		t.Error("expected wrong payload type to fail")
	}
}

func runHub(t *testing.T) *Hub {
	t.Helper()
	h := New()
	go h.Run()
	return h
}

func TestHub_NewClient_RegistersClient(t *testing.T) {
	h := runHub(t)
	c := h.NewClient()
	if c == nil {
		t.Fatal("expected non-nil client")
	}
	if c.Send == nil {
		t.Error("expected Send channel to be initialized")
	}
}

func TestHub_Broadcast_DeliveredToSubscriber(t *testing.T) {
	h := runHub(t)
	c := h.NewClient()
	h.AddScope(c, "sub1", Scope{Events: []EventType{EventPacketObservation}})

	// give hub time to process
	time.Sleep(10 * time.Millisecond)

	h.Broadcast(Event{Type: EventPacketObservation, IATA: "YVR"})

	select {
	case evt := <-c.Send:
		if evt.Type != EventPacketObservation {
			t.Errorf("expected packetObservation, got %s", evt.Type)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected event, timed out")
	}
}

func TestHub_Broadcast_NotDeliveredWithoutMatchingScope(t *testing.T) {
	h := runHub(t)
	c := h.NewClient()
	h.AddScope(c, "sub1", Scope{Events: []EventType{EventNodeUpdate}})

	time.Sleep(10 * time.Millisecond)

	h.Broadcast(Event{Type: EventPacketObservation, IATA: "YVR"})

	select {
	case <-c.Send:
		t.Error("expected no event for non-matching scope")
	case <-time.After(50 * time.Millisecond):
		// expected
	}
}

func TestHub_Broadcast_NoSubscriptions_NotDelivered(t *testing.T) {
	h := runHub(t)
	c := h.NewClient()

	time.Sleep(10 * time.Millisecond)

	h.Broadcast(Event{Type: EventPacketObservation, IATA: "YVR"})

	select {
	case <-c.Send:
		t.Error("expected no event for client with no subscriptions")
	case <-time.After(50 * time.Millisecond):
		// expected
	}
}

func TestHub_RemoveScope_StopsDelivery(t *testing.T) {
	h := runHub(t)
	c := h.NewClient()
	h.AddScope(c, "sub1", Scope{Events: []EventType{EventPacketObservation}})

	time.Sleep(10 * time.Millisecond)

	h.RemoveScope(c, "sub1")
	time.Sleep(20 * time.Millisecond) // wait for hub to process RemoveScope

	// drain anything that snuck in before removal was processed
	for len(c.Send) > 0 {
		<-c.Send
	}

	h.Broadcast(Event{Type: EventPacketObservation, IATA: "YVR"})

	select {
	case <-c.Send:
		t.Error("expected no event after scope removed")
	case <-time.After(50 * time.Millisecond):
		// expected
	}
}

func TestHub_Remove_ClosesChannels(t *testing.T) {
	h := runHub(t)
	c := h.NewClient()

	time.Sleep(10 * time.Millisecond)

	h.Remove(c)

	select {
	case _, ok := <-c.Send:
		if ok {
			t.Error("expected Send channel to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected Send channel to be closed, timed out")
	}
}

func TestHub_Broadcast_FullBuffer_SendsLaggedNotification(t *testing.T) {
	h := runHub(t)
	c := h.NewClient()
	h.AddScope(c, "sub1", Scope{Events: []EventType{EventPacketObservation}})

	time.Sleep(10 * time.Millisecond)

	// fill the send buffer
	for i := 0; i < cap(c.Send)+10; i++ {
		h.Broadcast(Event{Type: EventPacketObservation, IATA: "YVR"})
	}

	select {
	case notif := <-c.LaggedCH():
		if notif.DroppedCount < 1 {
			t.Errorf("expected DroppedCount >= 1, got %d", notif.DroppedCount)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected lagged notification, timed out")
	}
}

func TestScopeMatches_RouteTypeFilter(t *testing.T) {
	s := Scope{RouteTypes: []uint8{1}}
	if !scopeMatches(s, Event{Type: EventPacketObservation, RouteType: 1}) {
		t.Error("expected flood observation to match")
	}
	if scopeMatches(s, Event{Type: EventPacketObservation, RouteType: 2}) {
		t.Error("expected direct observation not to match")
	}
	if !scopeMatches(s, Event{Type: EventNodeUpdate}) {
		t.Error("routeTypes should only filter packetObservation")
	}
}

func TestScopeMatches_ObserverFilter(t *testing.T) {
	s := Scope{ObserverIDs: []string{"obs-a"}}
	for _, typ := range []EventType{EventPacketObservation, EventObserverStatus} {
		if !scopeMatches(s, Event{Type: typ, ObserverID: "obs-a"}) {
			t.Errorf("%s from obs-a should match", typ)
		}
		if scopeMatches(s, Event{Type: typ, ObserverID: "obs-b"}) {
			t.Errorf("%s from obs-b should not match", typ)
		}
	}
	if !scopeMatches(s, Event{Type: EventChannelMessage}) {
		t.Error("observerIds should not filter channelMessage")
	}
}

func TestHub_Broadcast_FullBuffer_KeepsNewestEvent(t *testing.T) {
	h := runHub(t)
	c := h.NewClient()
	h.AddScope(c, "sub1", Scope{Events: []EventType{EventPacketObservation}})
	time.Sleep(10 * time.Millisecond)

	for i := 0; i < cap(c.Send); i++ {
		h.Broadcast(Event{Type: EventPacketObservation, IATA: "YVR", Payload: json.RawMessage(`"old"`)})
	}
	h.Broadcast(Event{Type: EventPacketObservation, IATA: "YVR", Payload: json.RawMessage(`"new"`)})

	select {
	case notif := <-c.LaggedCH():
		if notif.DroppedCount != 1 {
			t.Errorf("DroppedCount = %d, want 1", notif.DroppedCount)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected lagged notification, timed out")
	}
	var last Event
	for len(c.Send) > 0 {
		last = <-c.Send
	}
	if string(last.Payload) != `"new"` {
		t.Errorf("newest queued event = %s, want the event that overflowed the buffer", last.Payload)
	}
}

func TestClientMatches_NoSubscriptions(t *testing.T) {
	c := &Client{subscriptions: make(map[string]Scope)}
	if c.matches(Event{Type: EventPacketObservation}) {
		t.Error("expected no match with empty subscriptions")
	}
}

func TestClientMatches_ORSemantics(t *testing.T) {
	c := &Client{
		subscriptions: map[string]Scope{
			"s1": {Events: []EventType{EventNodeUpdate}},
			"s2": {Events: []EventType{EventPacketObservation}},
		},
	}
	if !c.matches(Event{Type: EventPacketObservation}) {
		t.Error("expected match on second scope")
	}
	if !c.matches(Event{Type: EventNodeUpdate}) {
		t.Error("expected match on first scope")
	}
	if c.matches(Event{Type: EventChannelMessage}) {
		t.Error("expected no match for unsubscribed event type")
	}
}

func TestHub_ResolvePath_OptedIn_GetsResolvedPayload(t *testing.T) {
	h := runHub(t)
	c := h.NewClient()
	h.AddScope(c, "sub1", Scope{Events: []EventType{EventPacketObservation}})
	h.Configure(c, ClientOptions{ResolvePath: true})

	time.Sleep(10 * time.Millisecond)

	h.Broadcast(Event{
		Type:            EventPacketObservation,
		IATA:            "YVR",
		Payload:         json.RawMessage(`{"resolvedPath":null}`),
		PayloadResolved: json.RawMessage(`{"resolvedPath":[{"confidence":"high"}]}`),
	})

	select {
	case evt := <-c.Send:
		if string(evt.Payload) != `{"resolvedPath":[{"confidence":"high"}]}` {
			t.Errorf("expected resolved payload, got %s", evt.Payload)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected event, timed out")
	}
}

func TestHub_ResolvePath_DefaultOff_GetsBasePayload(t *testing.T) {
	h := runHub(t)
	c := h.NewClient()
	h.AddScope(c, "sub1", Scope{Events: []EventType{EventPacketObservation}})
	// no Configure call — default is off

	time.Sleep(10 * time.Millisecond)

	h.Broadcast(Event{
		Type:            EventPacketObservation,
		IATA:            "YVR",
		Payload:         json.RawMessage(`{"resolvedPath":null}`),
		PayloadResolved: json.RawMessage(`{"resolvedPath":[{"confidence":"high"}]}`),
	})

	select {
	case evt := <-c.Send:
		if string(evt.Payload) != `{"resolvedPath":null}` {
			t.Errorf("expected base payload (not opted in), got %s", evt.Payload)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected event, timed out")
	}
}

func TestHub_ResolvePath_OptedIn_NoResolvedVariant_FallsBackToBase(t *testing.T) {
	h := runHub(t)
	c := h.NewClient()
	// e.g. nodeUpdate events never carry a PayloadResolved variant
	h.AddScope(c, "sub1", Scope{Events: []EventType{EventNodeUpdate}})
	h.Configure(c, ClientOptions{ResolvePath: true})

	time.Sleep(10 * time.Millisecond)

	h.Broadcast(Event{
		Type:    EventNodeUpdate,
		IATA:    "YVR",
		Payload: json.RawMessage(`{"nodeId":"abc"}`),
		// PayloadResolved intentionally left nil
	})

	select {
	case evt := <-c.Send:
		if string(evt.Payload) != `{"nodeId":"abc"}` {
			t.Errorf("expected base payload as fallback, got %s", evt.Payload)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected event, timed out")
	}
}

func TestHub_ResolvePath_ToggleableLive(t *testing.T) {
	h := runHub(t)
	c := h.NewClient()
	h.AddScope(c, "sub1", Scope{Events: []EventType{EventPacketObservation}})

	broadcastAndRead := func() string {
		h.Broadcast(Event{
			Type:            EventPacketObservation,
			IATA:            "YVR",
			Payload:         json.RawMessage(`{"resolvedPath":null}`),
			PayloadResolved: json.RawMessage(`{"resolvedPath":[{"confidence":"high"}]}`),
		})
		select {
		case evt := <-c.Send:
			return string(evt.Payload)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("expected event, timed out")
			return ""
		}
	}

	time.Sleep(10 * time.Millisecond)
	if got := broadcastAndRead(); got != `{"resolvedPath":null}` {
		t.Errorf("expected base payload before opting in, got %s", got)
	}

	h.Configure(c, ClientOptions{ResolvePath: true})
	time.Sleep(10 * time.Millisecond)
	if got := broadcastAndRead(); got != `{"resolvedPath":[{"confidence":"high"}]}` {
		t.Errorf("expected resolved payload after opting in, got %s", got)
	}

	h.Configure(c, ClientOptions{})
	time.Sleep(10 * time.Millisecond)
	if got := broadcastAndRead(); got != `{"resolvedPath":null}` {
		t.Errorf("expected base payload after opting back out, got %s", got)
	}
}

func TestEvent_PayloadFor(t *testing.T) {
	full := Event{
		Payload:                json.RawMessage(`base`),
		PayloadResolved:        json.RawMessage(`resolved`),
		PayloadWithKey:         json.RawMessage(`key`),
		PayloadResolvedWithKey: json.RawMessage(`resolved+key`),
	}
	baseOnly := Event{Payload: json.RawMessage(`base`), PayloadResolved: json.RawMessage(`resolved`)}
	for _, tc := range []struct {
		name string
		evt  Event
		opts ClientOptions
		want string
	}{
		{"default", full, ClientOptions{}, "base"},
		{"resolve", full, ClientOptions{ResolvePath: true}, "resolved"},
		{"key", full, ClientOptions{IncludeObserverKey: true}, "key"},
		{"both", full, ClientOptions{ResolvePath: true, IncludeObserverKey: true}, "resolved+key"},
		{"key missing", baseOnly, ClientOptions{IncludeObserverKey: true}, "base"},
		{"both, key missing", baseOnly, ClientOptions{ResolvePath: true, IncludeObserverKey: true}, "resolved"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{ResolvePath: tc.opts.ResolvePath, IncludeObserverKey: tc.opts.IncludeObserverKey}
			if got := string(tc.evt.payloadFor(c)); got != tc.want {
				t.Fatalf("payloadFor = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestHub_ObserverKeyWanted_TracksOptIns(t *testing.T) {
	h := runHub(t)
	waitFor := func(want bool) {
		t.Helper()
		deadline := time.Now().Add(time.Second)
		for h.ObserverKeyWanted() != want {
			if time.Now().After(deadline) {
				t.Fatalf("ObserverKeyWanted = %t, want %t", !want, want)
			}
			time.Sleep(time.Millisecond)
		}
	}
	a, b := h.NewClient(), h.NewClient()
	if h.ObserverKeyWanted() {
		t.Fatal("no client opted in yet")
	}
	h.Configure(a, ClientOptions{IncludeObserverKey: true})
	h.Configure(a, ClientOptions{IncludeObserverKey: true}) // repeat must not double count
	h.Configure(b, ClientOptions{IncludeObserverKey: true})
	waitFor(true)
	h.Configure(a, ClientOptions{ResolvePath: true})
	h.Remove(b)
	waitFor(false)
}

func TestHub_Repeat_OnlyReachesOptedInClients(t *testing.T) {
	h := runHub(t)
	plain, opted := h.NewClient(), h.NewClient()
	h.AddScope(plain, "all", Scope{})
	h.AddScope(opted, "all", Scope{})
	h.Configure(opted, ClientOptions{IncludeRepeats: true})
	time.Sleep(10 * time.Millisecond)

	read := func(c *Client) (Event, bool) {
		select {
		case evt := <-c.Send:
			return evt, true
		case <-time.After(50 * time.Millisecond):
			return Event{}, false
		}
	}
	h.BroadcastRepeat(Event{Type: EventPacketObservation})
	if evt, ok := read(opted); !ok || !evt.Repeat {
		t.Fatalf("opted-in client: got %+v, %t", evt, ok)
	}
	if evt, ok := read(plain); ok {
		t.Fatalf("plain client got repeat %+v", evt)
	}
	h.Broadcast(Event{Type: EventPacketObservation})
	for _, c := range []*Client{plain, opted} {
		if evt, ok := read(c); !ok || evt.Repeat {
			t.Fatalf("normal event: got %+v, %t", evt, ok)
		}
	}
}

func TestHub_RepeatsWanted_TracksOptIns(t *testing.T) {
	h := runHub(t)
	waitFor := func(want bool) {
		t.Helper()
		deadline := time.Now().Add(time.Second)
		for h.RepeatsWanted() != want {
			if time.Now().After(deadline) {
				t.Fatalf("RepeatsWanted = %t, want %t", !want, want)
			}
			time.Sleep(time.Millisecond)
		}
	}
	a, b := h.NewClient(), h.NewClient()
	if h.RepeatsWanted() {
		t.Fatal("no client opted in yet")
	}
	h.Configure(a, ClientOptions{IncludeRepeats: true})
	h.Configure(a, ClientOptions{IncludeRepeats: true}) // repeat must not double count
	h.Configure(b, ClientOptions{IncludeRepeats: true})
	waitFor(true)
	h.Configure(a, ClientOptions{ResolvePath: true})
	h.Remove(b)
	waitFor(false)
}

func TestHub_BroadcastRepeat_DropsFirstWhenBusy(t *testing.T) {
	h := New() // not running, so the broadcast channel only fills
	h.BroadcastRepeat(Event{Type: EventPacketObservation})
	if len(h.broadcast) != 1 || h.repeatDrops.Load() != 0 {
		t.Fatalf("repeat on an idle hub: queued %d, dropped %d", len(h.broadcast), h.repeatDrops.Load())
	}
	for len(h.broadcast) < cap(h.broadcast)/2 {
		h.Broadcast(Event{Type: EventPacketObservation})
	}
	queued := len(h.broadcast)
	h.BroadcastRepeat(Event{Type: EventPacketObservation})
	h.BroadcastRepeat(Event{Type: EventPacketObservation})
	if len(h.broadcast) != queued || h.repeatDrops.Load() != 2 {
		t.Fatalf("repeats past half full: queued %d (want %d), dropped %d", len(h.broadcast), queued, h.repeatDrops.Load())
	}
	h.Broadcast(Event{Type: EventPacketObservation})
	if len(h.broadcast) != queued+1 {
		t.Fatal("normal event not enqueued past half full")
	}
}

func TestSentPaths(t *testing.T) {
	start := time.Now()
	s := newSentPaths(time.Minute, 3)
	hash, observer := []byte{1, 2}, []byte{3}
	if s.mark(hash, observer, []byte{0xaa}, start) {
		t.Fatal("first hearing counted as a repeat")
	}
	if s.mark(hash, observer, []byte{0xaa}, start.Add(time.Second)) {
		t.Fatal("broker copy of the first hearing counted as a repeat")
	}
	if !s.mark(hash, observer, []byte{0xaa, 0xbb}, start.Add(time.Second)) {
		t.Fatal("new path refused")
	}
	if s.mark(hash, observer, []byte{0xaa, 0xbb}, start.Add(time.Second)) {
		t.Fatal("exact copy within the window allowed")
	}
	if s.mark(hash, []byte{4}, []byte{0xaa}, start.Add(time.Second)) {
		t.Fatal("other observer's first hearing counted as a repeat")
	}
	if s.mark(hash, observer, []byte{0xcc}, start.Add(time.Minute)) {
		t.Fatal("hearing after the window not treated as a first hearing")
	}

	s = newSentPaths(time.Hour, 3)
	for i := range 4 {
		s.mark(hash, observer, []byte{byte(i)}, start)
	}
	if len(s.at) != 3 || len(s.order) != 3 {
		t.Fatalf("size bound: %d keys, %d queued", len(s.at), len(s.order))
	}
	if _, ok := s.at[s.key(hash, observer, []byte{0})]; ok {
		t.Fatal("oldest key not evicted at the size bound")
	}
}
