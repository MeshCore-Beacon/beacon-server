// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	mw "github.com/MeshCore-Beacon/beacon-server/internal/api/middleware"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

// MeRouter serves /api/v1/me. Every route needs a session; it expects mw.UserAuth upstream.
// MeMount adds routes to the signed-in router. They sit behind the session, forgery and read-only checks, and get
// the shared write limiter.
type MeMount func(r chi.Router, writes func(http.Handler) http.Handler)

func MeRouter(svc AccountsService, opts AuthOptions, mounts ...MeMount) http.Handler {
	if !opts.Enabled || svc == nil {
		return accountsDisabled()
	}
	r := chi.NewRouter()
	r.Use(accountsGate(opts))
	r.Use(mw.RequireCSRF(opts.PublicOrigin, opts.AllowedOrigins))
	r.Use(mw.RequireUser)
	writes := mw.RateLimitRoute(60, time.Minute) // one budget for every /me write
	r.Get("/", getMe())
	r.With(writes).Patch("/", patchMe(svc))
	if opts.Portal != nil {
		r.Post("/password", portalManaged)
		r.Post("/email", portalManaged)
	} else {
		r.With(writes).Post("/password", changePassword(svc))
		r.With(writes).Post("/email", changeEmail(svc))
	}
	r.With(writes).Delete("/", deleteMe(svc, opts))
	r.Get("/sessions", listSessions(svc))
	r.With(writes).Delete("/sessions", endOtherSessions(svc))
	r.With(writes).Delete("/sessions/{id}", endSession(svc, opts))
	for _, m := range mounts {
		if m != nil {
			m(r, writes)
		}
	}
	return r
}

// getMe godoc
// @Summary The signed-in user
// @Tags Accounts
// @Produce json
// @Security UserSession
// @Success 200 {object} api.UserResponse
// @Failure 401 {object} map[string]APIError
// @Router /me [get]
func getMe() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, _, _ := mw.UserFrom(r.Context())
		respond(w, http.StatusOK, api.UserResponse{User: api.NewUser(u)})
	}
}

// patchMe godoc
// @Summary Change the display name
// @Tags Accounts
// @Accept json
// @Produce json
// @Security UserSession
// @Param body body api.UpdateMeRequest true "1-40 characters after trimming; body at most 4 KiB"
// @Success 200 {object} api.UserResponse
// @Failure 400,401,403,413,415,422,429,503 {object} map[string]APIError
// @Router /me [patch]
func patchMe(svc AccountsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in api.UpdateMeRequest
		if !decodeBody(w, r, &in) {
			return
		}
		u, _, _ := mw.UserFrom(r.Context())
		updated, err := svc.SetDisplayName(r.Context(), u, in.DisplayName)
		if err != nil {
			userError(w, err)
			return
		}
		respond(w, http.StatusOK, api.UserResponse{User: api.NewUser(updated)})
	}
}

// changePassword godoc
// @Summary Change the password
// @Description Ends every other session of the user.
// @Tags Accounts
// @Accept json
// @Security UserSession
// @Param body body api.ChangePasswordRequest true "Current and new password; body at most 4 KiB"
// @Success 204 "Changed"
// @Failure 400,401,403,413,415,422,429,503 {object} map[string]APIError
// @Router /me/password [post]
func changePassword(svc AccountsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in api.ChangePasswordRequest
		if !decodeBody(w, r, &in) {
			return
		}
		u, s, _ := mw.UserFrom(r.Context())
		if err := svc.ChangePassword(r.Context(), u, s.ID, in.CurrentPassword, in.NewPassword); err != nil {
			userError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// changeEmail godoc
// @Summary Ask to change the email address
// @Description The new address is held as pendingEmail until its mailed link is used. Answers 202 check_email whether or not another user owns the address.
// @Tags Accounts
// @Accept json
// @Produce json
// @Security UserSession
// @Param body body api.ChangeEmailRequest true "New address and current password; body at most 4 KiB"
// @Success 202 {object} api.StatusResponse
// @Failure 400,401,403,413,415,422,429,503 {object} map[string]APIError
// @Router /me/email [post]
func changeEmail(svc AccountsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in api.ChangeEmailRequest
		if !decodeBody(w, r, &in) {
			return
		}
		u, _, _ := mw.UserFrom(r.Context())
		if err := svc.ChangeEmail(r.Context(), u, in.CurrentPassword, in.Email); err != nil {
			userError(w, err)
			return
		}
		respond(w, http.StatusAccepted, checkEmail())
	}
}

// deleteMe godoc
// @Summary Delete the account
// @Description Removes the user and everything it owns, and clears the cookie.
// @Tags Accounts
// @Accept json
// @Security UserSession
// @Param body body api.DeleteMeRequest true "currentPassword, or for a portal user confirmUsername; body at most 4 KiB"
// @Success 204 "Deleted"
// @Failure 400,401,403,413,415,429,503 {object} map[string]APIError
// @Router /me [delete]
func deleteMe(svc AccountsService, opts AuthOptions) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in api.DeleteMeRequest
		if !decodeBody(w, r, &in) {
			return
		}
		u, _, _ := mw.UserFrom(r.Context())
		confirm := in.CurrentPassword
		if u.Provider == users.ProviderPortal {
			confirm = in.ConfirmUsername
		}
		if err := svc.DeleteAccount(r.Context(), u, confirm); err != nil {
			userError(w, err)
			return
		}
		clearSessionCookie(w, opts)
		w.WriteHeader(http.StatusNoContent)
	}
}

// listSessions godoc
// @Summary List the user's sessions
// @Tags Accounts
// @Produce json
// @Security UserSession
// @Success 200 {object} api.SessionList
// @Failure 401 {object} map[string]APIError
// @Router /me/sessions [get]
func listSessions(svc AccountsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, cur, _ := mw.UserFrom(r.Context())
		list, err := svc.Sessions(r.Context(), u.ID)
		if err != nil {
			userError(w, err)
			return
		}
		out := make([]api.UserSession, 0, len(list))
		for _, s := range list {
			out = append(out, api.NewUserSession(s, s.ID == cur.ID))
		}
		respond(w, http.StatusOK, api.SessionList{Sessions: out})
	}
}

// endOtherSessions godoc
// @Summary End every session but this one
// @Tags Accounts
// @Security UserSession
// @Success 204 "Ended"
// @Failure 401,403,429,503 {object} map[string]APIError
// @Router /me/sessions [delete]
func endOtherSessions(svc AccountsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, cur, _ := mw.UserFrom(r.Context())
		if err := svc.EndOtherSessions(r.Context(), u.ID, cur.ID); err != nil {
			userError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// endSession godoc
// @Summary End one session
// @Description Ending the current session signs out and clears the cookie. An id that is not the user's is ignored.
// @Tags Accounts
// @Security UserSession
// @Param id path string true "Session UUID" format(uuid)
// @Success 204 "Ended"
// @Failure 401,403,422,429,503 {object} map[string]APIError
// @Router /me/sessions/{id} [delete]
func endSession(svc AccountsService, opts AuthOptions) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			userError(w, users.ErrInvalidInput)
			return
		}
		u, cur, _ := mw.UserFrom(r.Context())
		if err := svc.EndSession(r.Context(), u.ID, id); err != nil {
			userError(w, err)
			return
		}
		if id == cur.ID {
			clearSessionCookie(w, opts)
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
