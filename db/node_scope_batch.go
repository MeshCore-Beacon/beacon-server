// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
)

// GetNodeScopeEvidence enriches one already-paged node batch without neighbor IDs or history.
func (s *Store) GetNodeScopeEvidence(ctx context.Context, ids []uuid.UUID) (map[string]api.NodeScopeEvidence, error) {
	nodes := make([]api.NodeSummary, len(ids))
	for i, id := range ids {
		nodes[i] = api.NodeSummary{ID: id, NodeType: 2}
	}
	if err := s.enrichNodeScopes(ctx, nodes); err != nil {
		return nil, err
	}
	result := map[string]api.NodeScopeEvidence{}
	for _, n := range nodes {
		if n.ScopeEvidence != nil {
			result[n.ID.String()] = *n.ScopeEvidence
		}
	}
	return result, nil
}
