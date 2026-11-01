// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"slices"
	"time"
)

func (cr *CachedReader) GetScopeActivity(ctx context.Context, iatas []string, hours int) (*api.ScopeActivity, error) {
	names := slices.Clone(iatas)
	slices.Sort(names)
	names = slices.Compact(names)
	key, _ := json.Marshal(struct {
		Areas []string
		Hours int
		Hour  int64
	}{names, hours, cr.now().UTC().Truncate(time.Hour).Unix()})
	return getOrSet(ctx, cr.c, "scope-activity:v1:"+string(key), 30*time.Second, func(ctx context.Context) (*api.ScopeActivity, error) {
		return cr.inner.GetScopeActivity(ctx, names, hours)
	})
}

func (cr *CachedReader) GetNodeScopeEvidence(ctx context.Context, ids []uuid.UUID) (map[string]api.NodeScopeEvidence, error) {
	// Node pagination already bounds this key and request; no per-node queries.
	key, _ := json.Marshal(ids)
	return getOrSet(ctx, cr.c, "node-scope-batch:v1:"+scopeBatchKey(key), 30*time.Second, func(ctx context.Context) (map[string]api.NodeScopeEvidence, error) {
		return cr.inner.GetNodeScopeEvidence(ctx, ids)
	})
}

func scopeBatchKey(key []byte) string { sum := sha256.Sum256(key); return hex.EncodeToString(sum[:]) }
