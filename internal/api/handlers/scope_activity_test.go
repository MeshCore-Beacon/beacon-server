// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"net/http/httptest"
	"testing"
)

func (stubReader) GetScopeActivity(context.Context, []string, int) (*api.ScopeActivity, error) {
	return &api.ScopeActivity{Scopes: []api.ScopeActivityRow{}}, nil
}
func TestScopeActivityRejectsUnsupportedWindow(t *testing.T) {
	for _, v := range []string{"0", "7", "999999999", "bad"} {
		w := httptest.NewRecorder()
		getScopeActivity(stubReader{})(w, httptest.NewRequest("GET", "/scopes/activity?hours="+v, nil))
		if w.Code != 400 {
			t.Fatalf("%s: %d", v, w.Code)
		}
	}
}

func (stubReader) GetNodeScopeEvidence(context.Context, []uuid.UUID) (map[string]api.NodeScopeEvidence, error) {
	return map[string]api.NodeScopeEvidence{}, nil
}
