// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

// AtlasLimit is the most pins one Atlas holds.
const AtlasLimit = 12

const maxAtlasName = 160

const (
	PinNode     = "node"
	PinObserver = "observer"
)

var atlasTiles = map[string]bool{
	"airtime": true, "battery": true, "uptime": true, "types": true, "packets": true,
	"snr": true, "rssi": true, "noise": true, "queue": true, "errors": true,
}

// AtlasPin is one pin as a client reads it. A node pin carries publicKey, id and observerId; an observer pin
// carries observerPublicKey and observerId (null when the observer row is gone).
type AtlasPin struct {
	Kind              string   `json:"kind" enums:"node,observer"`
	PublicKey         string   `json:"publicKey,omitempty" example:"0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a"`
	ObserverPublicKey string   `json:"observerPublicKey,omitempty"`
	ID                *string  `json:"id" extensions:"x-nullable"`
	ObserverID        *string  `json:"observerId" extensions:"x-nullable"`
	Name              string   `json:"name"`
	Tiles             []string `json:"tiles,omitempty"`
	View              string   `json:"view,omitempty" enums:"heard,observer"`
}

// MarshalJSON leaves out the fields that do not belong to the pin's kind.
func (p AtlasPin) MarshalJSON() ([]byte, error) {
	m := map[string]any{"kind": p.Kind, "name": p.Name, "observerId": p.ObserverID}
	if p.Kind == PinNode {
		m["publicKey"], m["id"] = p.PublicKey, p.ID
		if p.View != "" {
			m["view"] = p.View
		}
	} else {
		m["observerPublicKey"] = p.ObserverPublicKey
	}
	if len(p.Tiles) > 0 {
		m["tiles"] = p.Tiles
	}
	return json.Marshal(m)
}

// Atlas is the answer to GET and PUT /me/atlas.
type Atlas struct {
	Revision int64      `json:"revision" example:"7"`
	Range    string     `json:"range" enums:"24h,3d"`
	Limit    int        `json:"limit" example:"12"`
	Pins     []AtlasPin `json:"pins"`
}

// AtlasConflict is the 409 body: the usual error plus the stored Atlas.
type AtlasConflict struct {
	Error   AtlasConflictError `json:"error"`
	Current Atlas              `json:"current"`
}

type AtlasConflictError struct {
	Code    string `json:"code" example:"atlas_conflict"`
	Message string `json:"message"`
}

// AtlasPinInput is one pin in a PUT. A node pin needs publicKey; an observer pin needs observerId or observerPublicKey.
type AtlasPinInput struct {
	Kind              string   `json:"kind" enums:"node,observer"`
	PublicKey         string   `json:"publicKey"`
	ObserverID        string   `json:"observerId"`
	ObserverPublicKey string   `json:"observerPublicKey"`
	Name              string   `json:"name"`
	Tiles             []string `json:"tiles"`
	View              *string  `json:"view"`
}

// PutAtlasRequest replaces the whole Atlas when BaseRevision is the stored revision.
type PutAtlasRequest struct {
	BaseRevision int64           `json:"baseRevision" example:"7"`
	Range        string          `json:"range" enums:"24h,3d"`
	Pins         []AtlasPinInput `json:"pins"`
}

// StoredPin is a pin as stored: always by public key.
type StoredPin struct {
	Kind  string
	Key   []byte
	Name  string
	Tiles []string
	View  string
}

// AtlasInvalid is a validation failure; the message names the pin.
type AtlasInvalid struct{ Message string }

func (e *AtlasInvalid) Error() string { return e.Message }

// ObserverIDs lists the observer ids a PUT refers to, so the caller can look their keys up before validating.
func (r PutAtlasRequest) ObserverIDs() []uuid.UUID {
	var ids []uuid.UUID
	for _, p := range r.Pins {
		if id, err := uuid.Parse(p.ObserverID); err == nil && p.Kind == PinObserver {
			ids = append(ids, id)
		}
	}
	return ids
}

func parseKey(s string) ([]byte, bool) {
	if len(s) != 64 {
		return nil, false
	}
	b, err := hex.DecodeString(s)
	return b, err == nil
}

// ValidateAtlasPut checks a PUT and returns the pins to store. observerKeys maps an observer id (lower case) to its
// public key for every id the request names that exists.
func ValidateAtlasPut(req PutAtlasRequest, observerKeys map[uuid.UUID][]byte) (string, []StoredPin, error) {
	if req.BaseRevision < 0 {
		return "", nil, &AtlasInvalid{"baseRevision must not be negative"}
	}
	if req.Range != "24h" && req.Range != "3d" {
		return "", nil, &AtlasInvalid{"range must be 24h or 3d"}
	}
	if len(req.Pins) > AtlasLimit {
		return "", nil, &AtlasInvalid{fmt.Sprintf("at most %d pins", AtlasLimit)}
	}
	out := make([]StoredPin, 0, len(req.Pins))
	seen := map[string]bool{}
	for i, in := range req.Pins {
		bad := func(msg string) error { return &AtlasInvalid{fmt.Sprintf("pin %d: %s", i+1, msg)} }
		sp := StoredPin{Kind: in.Kind, Name: in.Name}
		switch in.Kind {
		case PinNode:
			k, ok := parseKey(in.PublicKey)
			if !ok {
				return "", nil, bad("publicKey must be 64 hex characters")
			}
			sp.Key = k
		case PinObserver:
			if in.ObserverID == "" && in.ObserverPublicKey == "" {
				return "", nil, bad("observerId or observerPublicKey is required")
			}
			if in.ObserverPublicKey != "" {
				k, ok := parseKey(in.ObserverPublicKey)
				if !ok {
					return "", nil, bad("observerPublicKey must be 64 hex characters")
				}
				sp.Key = k
			}
			if in.ObserverID != "" {
				id, err := uuid.Parse(in.ObserverID)
				if err != nil {
					return "", nil, bad("observerId is not a valid id")
				}
				k, ok := observerKeys[id]
				if !ok {
					return "", nil, bad("unknown observerId")
				}
				if sp.Key != nil && hex.EncodeToString(sp.Key) != hex.EncodeToString(k) {
					return "", nil, bad("observerId and observerPublicKey name different observers")
				}
				sp.Key = k
			}
		default:
			return "", nil, bad("kind must be node or observer")
		}
		if utf8.RuneCountInString(in.Name) > maxAtlasName {
			return "", nil, bad(fmt.Sprintf("name is longer than %d characters", maxAtlasName))
		}
		if strings.ContainsRune(in.Name, 0) || !utf8.ValidString(in.Name) {
			return "", nil, bad("name has invalid characters")
		}
		if in.Tiles != nil {
			if len(in.Tiles) != 4 {
				return "", nil, bad("tiles must be exactly four")
			}
			dup := map[string]bool{}
			for _, t := range in.Tiles {
				if !atlasTiles[t] || dup[t] {
					return "", nil, bad("tiles must be four distinct known ids")
				}
				dup[t] = true
			}
			sp.Tiles = in.Tiles
		}
		if in.View != nil {
			if *in.View != "heard" && *in.View != "observer" {
				return "", nil, bad("view must be heard or observer")
			}
			if in.Kind == PinNode {
				sp.View = *in.View
			}
		}
		id := in.Kind + ":" + hex.EncodeToString(sp.Key)
		if seen[id] {
			return "", nil, bad("the same " + in.Kind + " is pinned twice")
		}
		seen[id] = true
		out = append(out, sp)
	}
	return req.Range, out, nil
}

// ResolveAtlas builds the answer from the stored pins. nodeIDs and observerIDs map a lower-case hex public key to
// the id of the row that has it now. An observer pin whose key now has a node row becomes a node pin in place; when
// that node is already pinned the two merge at the node pin's position.
func ResolveAtlas(rng string, revision int64, stored []StoredPin, nodeIDs, observerIDs map[string]uuid.UUID) Atlas {
	idStr := func(m map[string]uuid.UUID, k string) *string {
		if id, ok := m[k]; ok {
			s := id.String()
			return &s
		}
		return nil
	}
	nodePinned := map[string]bool{}
	for _, p := range stored {
		if p.Kind == PinNode {
			nodePinned[hex.EncodeToString(p.Key)] = true
		}
	}
	pins := make([]AtlasPin, 0, len(stored))
	for _, p := range stored {
		k := hex.EncodeToString(p.Key)
		switch {
		case p.Kind == PinNode:
			pins = append(pins, AtlasPin{Kind: PinNode, PublicKey: k, ID: idStr(nodeIDs, k), ObserverID: idStr(observerIDs, k),
				Name: p.Name, Tiles: p.Tiles, View: p.View})
		default:
			if _, isNode := nodeIDs[k]; isNode {
				if nodePinned[k] {
					continue
				}
				pins = append(pins, AtlasPin{Kind: PinNode, PublicKey: k, ID: idStr(nodeIDs, k), ObserverID: idStr(observerIDs, k),
					Name: p.Name, Tiles: p.Tiles})
				continue
			}
			pins = append(pins, AtlasPin{Kind: PinObserver, ObserverPublicKey: k, ObserverID: idStr(observerIDs, k),
				Name: p.Name, Tiles: p.Tiles})
		}
	}
	return Atlas{Revision: revision, Range: rng, Limit: AtlasLimit, Pins: pins}
}
