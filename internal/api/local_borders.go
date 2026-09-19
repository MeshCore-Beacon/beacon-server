// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"context"
	"slices"

	"github.com/MeshCore-Beacon/beacon-server/internal/borders"
	"github.com/google/uuid"
)

// WithLocalBorders annotates node responses after cache reads. Historical rows
// use the current startup configuration without extra SQL or cached-policy drift.
// Cached values are copied; the decorator never changes its inner reader's data.
func WithLocalBorders(reader Reader, local *borders.Local) Reader {
	if local == nil {
		return reader
	}
	return &localBorderReader{Reader: reader, local: local}
}

type localBorderReader struct {
	Reader
	local *borders.Local
}

func (r *localBorderReader) ListNodes(ctx context.Context, nodeType int16, iatas []string, paths, traces *bool, pubkey []byte, prefix, name, scope string, cursor int64, limit int32, neighbors bool) (Page[NodeSummary], error) {
	page, err := r.Reader.ListNodes(ctx, nodeType, iatas, paths, traces, pubkey, prefix, name, scope, cursor, limit, neighbors)
	if err != nil {
		return page, err
	}
	page.Items = slices.Clone(page.Items)
	for i := range page.Items {
		node := &page.Items[i]
		node.PossiblyForeign = r.local.PossiblyForeign(node.NodeType, node.Latitude, node.Longitude)
	}
	return page, nil
}

func (r *localBorderReader) GetNode(ctx context.Context, id uuid.UUID) (*Node, error) {
	node, err := r.Reader.GetNode(ctx, id)
	if err != nil || node == nil {
		return node, err
	}
	copy := *node
	copy.PossiblyForeign = r.local.PossiblyForeign(node.NodeType, node.Latitude, node.Longitude)
	return &copy, nil
}
