// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	mw "github.com/MeshCore-Beacon/beacon-server/internal/api/middleware"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
)

// RequestsService is the telemetry requests service the routes call. It is *polling.RequestService.
type RequestsService interface {
	Limit() int
	List(ctx context.Context, userID uuid.UUID) ([]polling.RequestRow, error)
	Create(ctx context.Context, userID, nodeID uuid.UUID, password []byte) (polling.RequestRow, error)
	Update(ctx context.Context, userID, id uuid.UUID, u polling.RequestUpdate) (polling.RequestRow, error)
	Delete(ctx context.Context, userID, id uuid.UUID) error
}

// RequestRoutes mounts /telemetry-requests on the /me router. A nil service answers 404 requests_disabled.
// Nothing here ever logs, returns or echoes a password.
func RequestRoutes(svc RequestsService) MeMount {
	return func(r chi.Router, writes func(http.Handler) http.Handler) {
		if svc == nil {
			off := func(w http.ResponseWriter, _ *http.Request) {
				respondCode(w, http.StatusNotFound, "requests_disabled", "Telemetry requests are not enabled on this server.")
			}
			r.HandleFunc("/telemetry-requests", off)
			r.HandleFunc("/telemetry-requests/*", off)
			return
		}
		r.Get("/telemetry-requests", listRequests(svc))
		r.With(writes).Post("/telemetry-requests", createRequest(svc))
		r.With(writes).Patch("/telemetry-requests/{id}", updateRequest(svc))
		r.With(writes).Delete("/telemetry-requests/{id}", deleteRequest(svc))
	}
}

// requestError answers an error from the requests service with the code clients act on.
func requestError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, polling.ErrRequestLimit):
		respondCode(w, http.StatusConflict, "request_limit", "You have reached the limit of telemetry requests.")
	case errors.Is(err, polling.ErrAlreadyRequested):
		respondCode(w, http.StatusConflict, "already_requested", "You already requested this repeater.")
	case errors.Is(err, polling.ErrNoPasswordBlocked):
		respondCode(w, http.StatusConflict, "no_password_blocked", "This repeater gives admin access to a blank login. Enter a guest password.")
	case errors.Is(err, polling.ErrNotARepeater):
		respondCode(w, http.StatusUnprocessableEntity, "not_a_repeater", "Only repeaters and room servers can be requested.")
	case errors.Is(err, polling.ErrPasswordInvalid):
		respondCode(w, http.StatusUnprocessableEntity, "password_invalid", "The password must be 1 to 15 bytes and contain no zero byte.")
	case errors.Is(err, polling.ErrRequestNotFound):
		respondCode(w, http.StatusNotFound, "not_found", "No such repeater or request.")
	default:
		slog.Error("telemetry request failed", "component", "api", "error", err)
		respondError(w, http.StatusInternalServerError, "internal error")
	}
}

// listRequests godoc
// @Summary The user's telemetry requests
// @Description Oldest first. The password is never returned; hasPassword says whether one is set. 404 requests_disabled when the server has requests off.
// @Tags Accounts
// @Produce json
// @Security UserSession
// @Success 200 {object} api.TelemetryRequestList
// @Failure 401,404 {object} map[string]APIError
// @Router /me/telemetry-requests [get]
func listRequests(svc RequestsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, _, _ := mw.UserFrom(r.Context())
		rows, err := svc.List(r.Context(), u.ID)
		if err != nil {
			requestError(w, err)
			return
		}
		out := api.TelemetryRequestList{Limit: svc.Limit(), Requests: make([]api.TelemetryRequest, 0, len(rows))}
		for _, row := range rows {
			out.Requests = append(out.Requests, api.NewTelemetryRequest(row))
		}
		respond(w, http.StatusOK, out)
	}
}

// createRequest godoc
// @Summary Ask for a repeater to be polled
// @Description The password is optional, 1 to 15 bytes with no zero byte, and is shared with volunteer observers: a guest password only, never an admin password. It is sealed before storage and never returned. Answers 201 with the request.
// @Tags Accounts
// @Accept json
// @Produce json
// @Security UserSession
// @Param body body api.CreateTelemetryRequest true "Body at most 4 KiB"
// @Success 201 {object} api.TelemetryRequest
// @Failure 400,401,403,404,413,415,429,503 {object} map[string]APIError
// @Failure 409 {object} map[string]APIError "request_limit, already_requested or no_password_blocked"
// @Failure 422 {object} map[string]APIError "invalid_input, not_a_repeater or password_invalid"
// @Router /me/telemetry-requests [post]
func createRequest(svc RequestsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in api.CreateTelemetryRequest
		if !decodeBody(w, r, &in) {
			return
		}
		nodeID, err := uuid.Parse(in.NodeID)
		if err != nil {
			respondCode(w, http.StatusUnprocessableEntity, "invalid_input", "nodeId must be a node id.")
			return
		}
		u, _, _ := mw.UserFrom(r.Context())
		row, err := svc.Create(r.Context(), u.ID, nodeID, passwordBytes(in.Password))
		if err != nil {
			requestError(w, err)
			return
		}
		respond(w, http.StatusCreated, api.NewTelemetryRequest(row))
	}
}

// passwordBytes maps an absent, null or empty password to nil: the blank or hello login.
func passwordBytes(p *string) []byte {
	if p == nil || *p == "" {
		return nil
	}
	return []byte(*p)
}

// updateRequest godoc
// @Summary Change a telemetry request
// @Description password sets a new guest password, or removes it when null; enabled true starts a disabled request again. At least one is required. A new password clears a failed or admin-stopped state. Answers 200 with the request.
// @Tags Accounts
// @Accept json
// @Produce json
// @Security UserSession
// @Param id path string true "Request UUID" format(uuid)
// @Param body body api.UpdateTelemetryRequest true "Body at most 4 KiB"
// @Success 200 {object} api.TelemetryRequest
// @Failure 400,401,403,404,413,415,429,503 {object} map[string]APIError
// @Failure 409 {object} map[string]APIError "no_password_blocked"
// @Failure 422 {object} map[string]APIError "invalid_input or password_invalid"
// @Router /me/telemetry-requests/{id} [patch]
func updateRequest(svc RequestsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			respondCode(w, http.StatusUnprocessableEntity, "invalid_input", "Not a request id.")
			return
		}
		var in api.UpdateTelemetryRequest
		if !decodeBody(w, r, &in) {
			return
		}
		if !in.Password.Set && in.Enabled == nil {
			respondCode(w, http.StatusUnprocessableEntity, "invalid_input", "Send a password or enabled.")
			return
		}
		if in.Enabled != nil && !*in.Enabled {
			respondCode(w, http.StatusUnprocessableEntity, "invalid_input", "enabled can only be true.")
			return
		}
		u, _, _ := mw.UserFrom(r.Context())
		row, err := svc.Update(r.Context(), u.ID, id, polling.RequestUpdate{
			SetPassword: in.Password.Set, Password: passwordBytes(in.Password.Value), Enable: in.Enabled != nil,
		})
		if err != nil {
			requestError(w, err)
			return
		}
		respond(w, http.StatusOK, api.NewTelemetryRequest(row))
	}
}

// deleteRequest godoc
// @Summary Remove a telemetry request
// @Description Deletes the request and its stored password. When it was the last request for the repeater, observers stop polling it.
// @Tags Accounts
// @Security UserSession
// @Param id path string true "Request UUID" format(uuid)
// @Success 204 "Removed"
// @Failure 401,403,404,429,503 {object} map[string]APIError
// @Router /me/telemetry-requests/{id} [delete]
func deleteRequest(svc RequestsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			respondCode(w, http.StatusUnprocessableEntity, "invalid_input", "Not a request id.")
			return
		}
		u, _, _ := mw.UserFrom(r.Context())
		if err := svc.Delete(r.Context(), u.ID, id); err != nil {
			requestError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
