// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
)

type statusCaptureDB struct {
	*stubDB
	status []UpdateObserverStatusParams
}

func (s *statusCaptureDB) UpdateObserverStatus(_ context.Context, p UpdateObserverStatusParams) (uuid.UUID, error) {
	s.status = append(s.status, p)
	return uuid.Nil, nil
}

func TestHandleStatus_RejectsInsaneRadio(t *testing.T) {
	for _, tc := range []struct {
		radio    string
		freq, bw float32
	}{
		{"910.5,62.5,7,5", 910.5, 62.5},
		{"910.5,NaN,7,5", 910.5, 0},
		{"Inf,62.5,7,5", 0, 62.5},
		{"910.5,1e-40,7,5", 910.5, 0},
		{"-910.5,-62.5,7,5", 0, 0},
		{"1e9,1e9,7,5", 0, 0},
	} {
		t.Run(tc.radio, func(t *testing.T) {
			w, base := newTestWorker()
			db := &statusCaptureDB{stubDB: base}
			w.db = db
			w.handleStatus(context.Background(), strings.Repeat("01", 32), []byte(`{"radio":"`+tc.radio+`"}`))
			if len(db.status) != 1 {
				t.Fatal("status not stored")
			}
			if got := db.status[0]; got.RadioFreqMHz != tc.freq || got.RadioBWKHz != tc.bw {
				t.Fatalf("radio stored as %v MHz / %v kHz, want %v / %v", got.RadioFreqMHz, got.RadioBWKHz, tc.freq, tc.bw)
			}
		})
	}
}

func TestHandlePacket_AdvertNameNULsStripped(t *testing.T) {
	w, base := newTestWorker()
	db := &frameCaptureDB{stubDB: base}
	w.db = db
	data := append([]byte{meshcore.AdvertTypeRepeater | meshcore.AdvertNameMask}, "Base\x00Camp\x00\x00"...)
	w.handlePacket(context.Background(), "YOW", "0102", packetEnvelope(t, buildAdvertPacketWithData(t, data, false)))
	if len(db.packets) != 1 || bytes.Contains(db.packets[0].ParsedPayload, []byte(`\u0000`)) {
		t.Fatalf("parsed payload kept NULs: %s", db.packets[0].ParsedPayload)
	}
	if base.upsertNodeCalls != 1 || base.upsertNodeParams.Name != "BaseCamp" {
		t.Fatalf("node name stored as %q", base.upsertNodeParams.Name)
	}
}

type neighborCaptureDB struct {
	*stubDB
	selfScope string
	scopes    []string
}

func (s *neighborCaptureDB) GetNodeByPubkey(_ context.Context, pubkey []byte) (uuid.UUID, error) {
	if len(pubkey) == 0 {
		return uuid.Nil, errors.New("not found")
	}
	return uuid.UUID{pubkey[0]}, nil
}

func (s *neighborCaptureDB) UpdateObserverRegionScope(_ context.Context, _ uuid.UUID, scope string) error {
	s.selfScope = scope
	return nil
}

func (s *neighborCaptureDB) UpsertNodeNeighbor(_ context.Context, _, _ uuid.UUID, _ string, _ *float32, scope *string) error {
	if scope != nil {
		s.scopes = append(s.scopes, *scope)
	}
	return nil
}

func TestHandleNeighbors_ScopeNULsStripped(t *testing.T) {
	w, base := newTestWorker()
	db := &neighborCaptureDB{stubDB: base}
	w.db = db
	report := `{"self":{"scopes":"#ott\u0000"},"neighbors":[{"pubkey":"` + strings.Repeat("02", 32) + `","status":"responded","scopes":"#y\u0000ow"}]}`
	w.handleNeighbors(context.Background(), "YOW", strings.Repeat("01", 32), []byte(report))
	if db.selfScope != "#ott" || len(db.scopes) != 1 || db.scopes[0] != "#yow" {
		t.Fatalf("scopes stored as %q %q", db.selfScope, db.scopes)
	}
}
