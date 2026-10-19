// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package guards

import (
	"errors"
	"net/http/httptest"
	"slices"
	"testing"
	"time"
)

func TestGuardFamilyClassify(t *testing.T) {
	for _, c := range []struct {
		url string
		f   Family
		cl  Class
	}{
		{"/api/v1/packets", PacketsGlobal, List}, {"/api/v1/packets?hashPrefix=abcd", PacketsGlobal, List},
		{"/api/v1/packets?iatas=YVR", PacketsRegion, List}, {"/api/v1/packets?region=bc", PacketsRegion, List},
		{"/api/v1/packets?since=1", PacketsWindow, Aggregation}, {"/api/v1/packets?iatas=YVR&until=2", PacketsWindow, Aggregation},
		{"/api/v1/packets?since=", PacketsGlobal, List}, {"/api/v1/packets/backfill?afterObservationId=1&iatas=YVR", PacketsRegion, List},
		{"/api/v1/packets/backfill?since=1", PacketsGlobal, List}, {"/api/v1/packets?regionId=3", PacketsRegion, List},
		{"/api/v1/packets?since=&since=x", PacketsGlobal, List}, {"/api/v1/packets?since=x&since=", PacketsWindow, Aggregation},
		{"/api/v1/packets?iata=&iata=YOW", PacketsGlobal, List}, {"/api/v1/packets?iatas=,", PacketsGlobal, List},
		{"/api/v1/packets?iatas=,&iata=YOW", PacketsGlobal, List}, {"/api/v1/packets?iatas=&iata=YOW", PacketsRegion, List},
		{"/api/v1/packets?iata=%20", PacketsRegion, List}, {"/api/v1/packets?region=&region=bc", PacketsGlobal, List},
		{"/api/v1/observers/directory?iatas=,", Other, Aggregation},
		{"/api/v1/packets/ab12", PacketDetail, Entity},
		{"/api/v1/routes?iata=YVR", Routes, List}, {"/api/v1/routes/search?hash=ab", Routes, Aggregation},
		{"/api/v1/routes/cross", Routes, Aggregation}, {"/api/v1/routes/YVR/abcd/observations", Routes, Entity},
		{"/api/v1/channels/3/messages", ChannelMessages, List}, {"/api/v1/messages?since=1", ChannelMessages, List},
		{"/api/v1/messages/backfill", ChannelMessages, List},
		{"/api/v1/stats/series", Stats, Aggregation}, {"/api/v1/stats/node-types", Stats, Aggregation},
		{"/api/v1/observers/directory", Other, Aggregation}, {"/api/v1/observers/directory?iatas=YVR", Other, List},
		{"/api/v1/observers/x/activity", Other, Aggregation}, {"/api/v1/observers/x/adverts", Other, List},
		{"/api/v1/observers/x/telemetry", Other, Entity}, {"/api/v1/observers/x", Other, Entity},
		{"/api/v1/observers", Other, List}, {"/api/v1/nodes", Other, List},
		{"/api/v1/nodes/x/observations", Other, List}, {"/api/v1/nodes/x/neighbors", Other, Entity},
		{"/api/v1/nodes/x", Other, Entity}, {"/api/v1/channels", Other, List}, {"/api/v1/channels/3", Other, Entity},
		{"/api/v1/traces", Other, List}, {"/api/v1/traces/x", Other, Entity}, {"/api/v1/iatas", Other, Entity},
		{"/api/v1/regions/x", Other, Entity}, {"/api/v1/scopes", Other, Entity}, {"/api/v1/brokers", Other, Entity},
		{"/api/v1/info", Other, Entity}, {"/api/v1/nope", Other, Entity},
	} {
		f, cl := Classify(httptest.NewRequest("GET", c.url, nil))
		if f != c.f || cl != c.cl {
			t.Errorf("Classify(%s) = (%s, %s), want (%s, %s)", c.url, f, cl, c.f, c.cl)
		}
	}
}

func TestParseFamilies(t *testing.T) {
	got, err := ParseFamilies([]string{" stats", "routes", "stats"})
	if err != nil || !slices.Equal(got, []Family{Routes, Stats}) {
		t.Errorf("ParseFamilies = %v, %v; want [routes stats]", got, err)
	}
	if _, err := ParseFamilies([]string{"x"}); !errors.Is(err, ErrUnknownFamily) {
		t.Errorf("ParseFamilies([x]) error = %v, want ErrUnknownFamily", err)
	}
	got, err = ParseFamilies([]string{})
	if err != nil || got == nil || len(got) != 0 {
		t.Errorf("ParseFamilies([]) = %#v, %v; want empty", got, err)
	}
}

func TestClassDeadlines(t *testing.T) {
	for c, want := range map[Class]time.Duration{Entity: 5 * time.Second, List: 10 * time.Second, Aggregation: 30 * time.Second} {
		if c.Deadline() != want {
			t.Errorf("%s.Deadline() = %s, want %s", c, c.Deadline(), want)
		}
	}
	if Entity.String() != "entity" || List.String() != "list" || Aggregation.String() != "aggregation" {
		t.Error("Class.String mismatch")
	}
}

func TestEnforceIsASet(t *testing.T) {
	g := New(Options{Enforce: []Family{Stats, Routes, Stats}})
	if !slices.Equal(g.Enforce(), []Family{Routes, Stats}) {
		t.Errorf("Enforce() = %v, want [routes stats]", g.Enforce())
	}
	if len(New(Options{}).Enforce()) != 0 {
		t.Error("default Enforce() is not empty")
	}
}
