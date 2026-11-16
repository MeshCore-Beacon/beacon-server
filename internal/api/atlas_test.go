// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func key(b byte) string { return strings.Repeat(fmt.Sprintf("%02x", b), 32) }

func kb(b byte) []byte { k, _ := hex.DecodeString(key(b)); return k }

func str(s string) *string { return &s }

func TestValidateAtlasPut(t *testing.T) {
	obs := uuid.New()
	known := map[uuid.UUID][]byte{obs: kb(9)}
	tiles := []string{"airtime", "battery", "types", "noise"}
	ttiles := []string{"battery", "noise", "uptime", "packets"}
	node := func(b byte) AtlasPinInput { return AtlasPinInput{Kind: "node", PublicKey: key(b), Name: "n"} }
	many := make([]AtlasPinInput, 13)
	for i := range many {
		many[i] = node(byte(i + 1))
	}
	cases := []struct {
		name string
		req  PutAtlasRequest
		want string // substring of the error; empty means valid
	}{
		{"ok", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{node(1), {Kind: "observer", ObserverID: obs.String()}, {Kind: "observer", ObserverPublicKey: key(2)}}}, ""},
		{"empty", PutAtlasRequest{Range: "3d"}, ""},
		{"tiles and view", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), Tiles: tiles, View: str("observer")}}}, ""},
		{"telemetry view", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), View: str("telemetry")}}}, ""},
		{"telemetry tiles", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), TelemetryTiles: ttiles}}}, ""},
		{"telemetry tiles with tiles", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), Tiles: tiles, TelemetryTiles: ttiles, View: str("telemetry")}}}, ""},
		{"three telemetry tiles", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), TelemetryTiles: ttiles[:3]}}}, "telemetryTiles"},
		{"empty telemetry tiles", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), TelemetryTiles: []string{}}}}, "telemetryTiles"},
		{"repeated telemetry tile", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), TelemetryTiles: []string{"battery", "battery", "noise", "uptime"}}}}, "telemetryTiles"},
		{"unknown telemetry tile", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), TelemetryTiles: []string{"battery", "noise", "uptime", "snr"}}}}, "telemetryTiles"},
		{"telemetry tiles on observer", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "observer", ObserverPublicKey: key(2), TelemetryTiles: ttiles}}}, "telemetryTiles"},
		{"negative base", PutAtlasRequest{BaseRevision: -1, Range: "24h"}, "baseRevision"},
		{"range", PutAtlasRequest{Range: "7d"}, "range"},
		{"no range", PutAtlasRequest{}, "range"},
		{"thirteen pins", PutAtlasRequest{Range: "24h", Pins: many}, "at most 12"},
		{"kind", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "x", PublicKey: key(1)}}}, "pin 1: kind"},
		{"short key", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{node(1), {Kind: "node", PublicKey: "abcd"}}}, "pin 2: publicKey"},
		{"non hex key", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: strings.Repeat("zz", 32)}}}, "pin 1: publicKey"},
		{"long name", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), Name: strings.Repeat("é", 161)}}}, "name"},
		{"name at limit", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), Name: strings.Repeat("é", 160)}}}, ""},
		{"nul in name", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), Name: "a\x00b"}}}, "name"},
		{"three tiles", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), Tiles: tiles[:3]}}}, "tiles"},
		{"empty tiles", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), Tiles: []string{}}}}, "tiles"},
		{"repeated tile", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), Tiles: []string{"snr", "snr", "rssi", "noise"}}}}, "tiles"},
		{"unknown tile", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), Tiles: []string{"snr", "rssi", "noise", "bogus"}}}}, "tiles"},
		{"view", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "node", PublicKey: key(1), View: str("both")}}}, "view"},
		{"duplicate node", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{node(1), node(1)}}, "pin 2: the same node"},
		{"node and observer same key", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{node(1), {Kind: "observer", ObserverPublicKey: key(1)}}}, ""},
		{"duplicate observer by both forms", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "observer", ObserverID: obs.String()}, {Kind: "observer", ObserverPublicKey: key(9)}}}, "pin 2: the same observer"},
		{"unknown observer id", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "observer", ObserverID: uuid.NewString()}}}, "pin 1: unknown observerId"},
		{"bad observer id", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "observer", ObserverID: "nope"}}}, "observerId"},
		{"observer without ref", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "observer"}}}, "observerId or observerPublicKey"},
		{"observer refs disagree", PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "observer", ObserverID: obs.String(), ObserverPublicKey: key(3)}}}, "different"},
	}
	for _, c := range cases {
		_, _, err := ValidateAtlasPut(c.req, known)
		if c.want == "" && err != nil || c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)) {
			t.Errorf("%s: err=%v want %q", c.name, err, c.want)
		}
	}
	// The observer id is stored by its key.
	_, pins, _ := ValidateAtlasPut(PutAtlasRequest{Range: "24h", Pins: []AtlasPinInput{{Kind: "observer", ObserverID: obs.String(), Name: "N"}}}, known)
	if hex.EncodeToString(pins[0].Key) != key(9) || pins[0].Kind != "observer" {
		t.Fatalf("stored %+v", pins[0])
	}
}

func TestResolveAtlas(t *testing.T) {
	n1, n2, o1 := uuid.New(), uuid.New(), uuid.New()
	tiles := []string{"a", "b", "c", "d"}
	stored := []StoredPin{
		{Kind: "node", Key: kb(1), Name: "known", Tiles: tiles, View: "telemetry", TelemetryTiles: tiles},
		{Kind: "node", Key: kb(2), Name: "unknown"},
		{Kind: "observer", Key: kb(3), Name: "also node"},         // gained a node row: upgraded in place
		{Kind: "node", Key: kb(4), Name: "pinned both ways"},      // node pin first
		{Kind: "observer", Key: kb(4), Name: "dup", Tiles: tiles}, // merges into the node pin above
		{Kind: "observer", Key: kb(5), Name: "plain observer"},
		{Kind: "observer", Key: kb(6), Name: "gone"},
	}
	nodes := map[string]uuid.UUID{key(1): n1, key(3): n2, key(4): uuid.New()}
	observers := map[string]uuid.UUID{key(1): o1, key(5): uuid.New()}
	a := ResolveAtlas("3d", 7, stored, nodes, observers)
	if a.Revision != 7 || a.Range != "3d" || a.Limit != 12 || len(a.Pins) != 6 {
		t.Fatalf("%+v", a)
	}
	p := a.Pins
	if p[0].ID == nil || *p[0].ID != n1.String() || p[0].ObserverID == nil || *p[0].ObserverID != o1.String() || p[0].View != "telemetry" || len(p[0].TelemetryTiles) != 4 {
		t.Errorf("known node: %+v", p[0])
	}
	if p[1].Kind != "node" || p[1].ID != nil || p[1].ObserverID != nil {
		t.Errorf("unknown node: %+v", p[1])
	}
	if p[2].Kind != "node" || p[2].PublicKey != key(3) || p[2].ID == nil || *p[2].ID != n2.String() || p[2].Name != "also node" {
		t.Errorf("upgrade in place: %+v", p[2])
	}
	if p[3].Kind != "node" || p[3].PublicKey != key(4) || p[3].Name != "pinned both ways" {
		t.Errorf("merge keeps the node pin position: %+v", p[3])
	}
	if p[4].Kind != "observer" || p[4].ObserverPublicKey != key(5) || p[4].ObserverID == nil {
		t.Errorf("observer: %+v", p[4])
	}
	if p[5].Kind != "observer" || p[5].ObserverID != nil {
		t.Errorf("observer without a row: %+v", p[5])
	}
	e := ResolveAtlas("24h", 0, nil, nil, nil)
	b, _ := json.Marshal(e)
	if string(b) != `{"revision":0,"range":"24h","limit":12,"pins":[]}` {
		t.Errorf("empty: %s", b)
	}
	b, _ = json.Marshal(a.Pins[0])
	if !strings.Contains(string(b), `"view":"telemetry"`) || !strings.Contains(string(b), `"telemetryTiles":["a","b","c","d"]`) {
		t.Errorf("node json: %s", b)
	}
	b, _ = json.Marshal(a.Pins[1])
	if strings.Contains(string(b), "telemetryTiles") {
		t.Errorf("unset telemetryTiles is left out: %s", b)
	}
	// JSON keeps each kind's own fields.
	b, _ = json.Marshal(a.Pins[4])
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if _, has := m["publicKey"]; has || m["observerPublicKey"] != key(5) {
		t.Errorf("observer json: %s", b)
	}
}
