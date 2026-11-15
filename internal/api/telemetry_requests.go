// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"bytes"
	"encoding/hex"
	"encoding/json"

	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
)

// TelemetryRequestNode is the repeater a request names. ID is null when the server no longer has the node.
type TelemetryRequestNode struct {
	ID        *string `json:"id" example:"3f2b8c1e-6a52-4d0e-9b57-0d3d2b6a1c11"`
	PublicKey string  `json:"publicKey" example:"0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a"`
	Name      string  `json:"name" example:"Hilltop"`
}

// TelemetryRequestPoller is the observer polling the repeater.
type TelemetryRequestPoller struct {
	ObserverID string `json:"observerId"`
	Name       string `json:"name" example:"North Ridge"`
}

// TelemetryRequest is one request. The password is never part of it: HasPassword only says whether one is set.
type TelemetryRequest struct {
	ID           string               `json:"id"`
	Node         TelemetryRequestNode `json:"node"`
	Status       string               `json:"status" enums:"waiting,active,login_failed,admin_stopped,disabled"`
	StatusReason string               `json:"statusReason" example:"No observer can reach this repeater yet."`
	// StatusReasonCode is a stable code for the reason: no_observer, no_seat, login_failed, blank_login_failed,
	// admin_stopped, blank_blocked, offline or other; empty when there is no reason.
	StatusReasonCode string `json:"statusReasonCode" example:"no_observer"`
	// StatusReasonDays is the number of days of the offline reason; omitted for any other.
	StatusReasonDays *int                    `json:"statusReasonDays,omitempty" example:"7"`
	HasPassword      bool                    `json:"hasPassword"`
	CreatedAt        int64                   `json:"createdAt" example:"1794700000000"`
	UpdatedAt        int64                   `json:"updatedAt" example:"1794700000000"`
	Poller           *TelemetryRequestPoller `json:"poller"`
	// LatestReadingAt is epoch ms of the newest trusted reading, null when none.
	LatestReadingAt *int64 `json:"latestReadingAt" example:"1794703600000"`
}

// TelemetryRequestList is the answer to GET /me/telemetry-requests.
type TelemetryRequestList struct {
	// Limit is how many requests one user may hold.
	Limit    int                `json:"limit" example:"3"`
	Requests []TelemetryRequest `json:"requests"`
}

// CreateTelemetryRequest asks for a repeater to be polled. Password is optional, 1 to 15 bytes, and a guest password
// only: an admin password must never be entered.
type CreateTelemetryRequest struct {
	NodeID   string  `json:"nodeId" example:"3f2b8c1e-6a52-4d0e-9b57-0d3d2b6a1c11"`
	Password *string `json:"password"`
}

// OptionalString tells an absent JSON field from a null or a value.
type OptionalString struct {
	Set   bool
	Value *string
}

func (o *OptionalString) UnmarshalJSON(b []byte) error {
	o.Set = true
	if bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
		o.Value = nil
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	o.Value = &s
	return nil
}

// UpdateTelemetryRequest changes a request: a new password (null removes it) and/or enabled true to start a
// disabled request again.
type UpdateTelemetryRequest struct {
	Password OptionalString `json:"password" swaggertype:"string"`
	Enabled  *bool          `json:"enabled"`
}

// NewTelemetryRequest converts a stored request for a response.
func NewTelemetryRequest(r polling.RequestRow) TelemetryRequest {
	out := TelemetryRequest{
		ID:     r.ID.String(),
		Node:   TelemetryRequestNode{PublicKey: hex.EncodeToString(r.Target), Name: r.NodeName},
		Status: string(r.Status), StatusReason: r.Reason, HasPassword: r.HasPassword,
		CreatedAt: r.CreatedAt.UnixMilli(), UpdatedAt: r.UpdatedAt.UnixMilli(),
	}
	code, days := polling.ReasonCode(r.Reason)
	out.StatusReasonCode = code
	if code == polling.CodeOffline {
		out.StatusReasonDays = &days
	}
	if r.NodeID != nil {
		s := r.NodeID.String()
		out.Node.ID = &s
	}
	if r.PollerID != nil {
		out.Poller = &TelemetryRequestPoller{ObserverID: r.PollerID.String(), Name: r.PollerName}
	}
	if r.LatestAt != nil {
		ms := r.LatestAt.UnixMilli()
		out.LatestReadingAt = &ms
	}
	return out
}
