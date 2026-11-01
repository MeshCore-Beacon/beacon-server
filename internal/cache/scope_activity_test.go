// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"context"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
)

func (*stubReader) GetScopeActivity(context.Context, []string, int) (*api.ScopeActivity, error) {
	return &api.ScopeActivity{}, nil
}

func (*stubReader) GetNodeScopeEvidence(context.Context, []uuid.UUID) (map[string]api.NodeScopeEvidence, error) {
	return map[string]api.NodeScopeEvidence{}, nil
}
