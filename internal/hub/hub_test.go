// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: agpl

package hub

import "testing"

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
