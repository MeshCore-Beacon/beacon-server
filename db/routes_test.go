// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
)

func TestExtractFromNode_Found(t *testing.T) {
	a, b, c := uuid.New(), uuid.New(), uuid.New()
	hops := []api.RouteHop{
		{NodeID: a},
		{NodeID: b},
		{NodeID: c},
	}
	result := extractFromNode(hops, b)
	if len(result) != 2 {
		t.Fatalf("expected 2 hops, got %d", len(result))
	}
	if result[0].NodeID != b {
		t.Errorf("expected first hop to be b, got %s", result[0].NodeID)
	}
	if result[1].NodeID != c {
		t.Errorf("expected second hop to be c, got %s", result[1].NodeID)
	}
}

func TestExtractFromNode_FirstNode(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	hops := []api.RouteHop{{NodeID: a}, {NodeID: b}}
	result := extractFromNode(hops, a)
	if len(result) != 2 {
		t.Fatalf("expected 2 hops, got %d", len(result))
	}
}

func TestExtractFromNode_NotFound(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	hops := []api.RouteHop{{NodeID: a}}
	result := extractFromNode(hops, b)
	// not found returns full slice
	if len(result) != 1 {
		t.Fatalf("expected full slice returned, got %d hops", len(result))
	}
}

func TestExtractFromNode_Empty(t *testing.T) {
	result := extractFromNode(nil, uuid.New())
	if len(result) != 0 {
		t.Errorf("expected empty result for nil hops")
	}
}
