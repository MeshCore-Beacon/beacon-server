// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"encoding/json"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"io"
	"net/http"
)

type NodeScopeBatchRequest struct {
	NodeIDs []uuid.UUID `json:"nodeIds"`
}

// nodeScopeBatch godoc
// @Summary Read scope evidence for a bounded node batch
// @Description Read-only POST avoids long URLs. Accepts at most 1000 IDs and 64 KiB. Does not poll radios or change server state.
// @Tags Nodes
// @Accept json
// @Produce json
// @Param body body NodeScopeBatchRequest true "Node IDs"
// @Success 200 {object} map[string]api.NodeScopeEvidence
// @Failure 400 {object} handlers.APIError
// @Failure 500 {object} handlers.APIError
// @Router /nodes/scopes [post]
func nodeScopeBatch(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
		defer r.Body.Close()
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var input NodeScopeBatchRequest
		if err := decoder.Decode(&input); err != nil || len(input.NodeIDs) == 0 || len(input.NodeIDs) > 1000 {
			respondError(w, http.StatusBadRequest, "expected 1 to 1000 nodeIds within 64 KiB")
			return
		}
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			respondError(w, http.StatusBadRequest, "expected one JSON object")
			return
		}
		ids := make([]uuid.UUID, 0, len(input.NodeIDs))
		seen := map[uuid.UUID]bool{}
		for _, id := range input.NodeIDs {
			if id == uuid.Nil {
				respondError(w, http.StatusBadRequest, "invalid node ID")
				return
			}
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
		result, err := reader.GetNodeScopeEvidence(r.Context(), ids)
		if err != nil {
			respondReaderError(w, err, "failed to load scope evidence")
			return
		}
		respond(w, http.StatusOK, result)
	}
}
