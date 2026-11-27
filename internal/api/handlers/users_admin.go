// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

// UsersAdminStore is what the operator's user routes need; *db.Store satisfies it.
type UsersAdminStore interface {
	ListUsers(ctx context.Context, search string, afterCreated time.Time, afterID uuid.UUID, limit int) ([]users.User, error)
	UserByID(ctx context.Context, id uuid.UUID) (users.User, error)
	SetUserDisabled(ctx context.Context, id uuid.UUID, at *time.Time) error
	EndAllSessions(ctx context.Context, userID uuid.UUID) error
	DeleteUser(ctx context.Context, id uuid.UUID) error
}

const (
	usersPageDefault = 50
	usersPageMax     = 200
	usersSearchMax   = 100
)

// UsersAdminRouter must be mounted inside the authenticated admin subtree. With a nil store (accounts off) or on a
// read-only instance (readOnly) every path answers 503.
func UsersAdminRouter(store UsersAdminStore, readOnly bool) http.Handler {
	if store == nil || readOnly {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			respondError(w, http.StatusServiceUnavailable, "user management is unavailable")
		})
	}
	r := chi.NewRouter()
	r.Get("/", listUsers(store))
	r.Put("/{id}", setUserDisabled(store))
	r.Delete("/{id}", deleteUser(store))
	return r
}

// listUsers godoc
// @Summary List users
// @Description Newest first. The password hash is never returned.
// @Tags Admin
// @Produce json
// @Security AdminKey
// @Param search query string false "Substring of the username or email, ignoring case (at most 100 characters)"
// @Param cursor query string false "nextCursor of the previous page"
// @Param limit query int false "Page size, 1-200" default(50)
// @Success 200 {object} api.AdminUserList
// @Failure 400,401,503,500 {object} map[string]APIError
// @Router /admin/users [get]
func listUsers(store UsersAdminStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		search := q.Get("search")
		if len(search) > usersSearchMax {
			respondError(w, http.StatusBadRequest, "search is too long")
			return
		}
		limit := usersPageDefault
		if raw := q.Get("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 1 || n > usersPageMax {
				respondError(w, http.StatusBadRequest, "limit must be between 1 and 200")
				return
			}
			limit = n
		}
		// The first page starts after every user.
		afterCreated, afterID := time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")
		if raw := q.Get("cursor"); raw != "" {
			c, err := api.ParseUserCursor(raw)
			if err != nil {
				respondError(w, http.StatusBadRequest, "invalid cursor")
				return
			}
			afterCreated, afterID = c.CreatedAt, c.ID
		}
		rows, err := store.ListUsers(r.Context(), search, afterCreated, afterID, limit+1)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "could not list users")
			return
		}
		out := api.AdminUserList{Items: make([]api.AdminUser, 0, len(rows))}
		if len(rows) > limit {
			rows = rows[:limit]
			last := rows[len(rows)-1]
			out.NextCursor = api.UserCursor{CreatedAt: last.CreatedAt, ID: last.ID}.String()
		}
		for _, u := range rows {
			out.Items = append(out.Items, api.NewAdminUser(u))
		}
		respond(w, http.StatusOK, out)
	}
}

func userID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid user ID")
		return uuid.Nil, false
	}
	return id, true
}

func adminUserError(w http.ResponseWriter, err error) {
	if errors.Is(err, users.ErrNotFound) {
		respondError(w, http.StatusNotFound, "user not found")
		return
	}
	respondError(w, http.StatusInternalServerError, "could not update the user")
}

// setUserDisabled godoc
// @Summary Disable or enable a user
// @Description Disabling ends every session of the user at once and refuses new sign-ins; enabling lifts it.
// @Tags Admin
// @Accept json
// @Produce json
// @Security AdminKey
// @Param id path string true "User UUID" format(uuid)
// @Param body body api.SetUserDisabledRequest true "Body at most 4 KiB"
// @Success 200 {object} api.AdminUserResponse
// @Failure 400,401,404,413,415,503,500 {object} map[string]APIError
// @Router /admin/users/{id} [put]
func setUserDisabled(store UsersAdminStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := userID(w, r)
		if !ok {
			return
		}
		var in api.SetUserDisabledRequest
		if !decodeBody(w, r, &in) {
			return
		}
		var at *time.Time
		if in.Disabled {
			now := time.Now()
			at = &now
		}
		if err := store.SetUserDisabled(r.Context(), id, at); err != nil {
			adminUserError(w, err)
			return
		}
		// A disabled user cannot authenticate anyway; ending the sessions keeps the rows from lingering as live.
		if in.Disabled {
			if err := store.EndAllSessions(r.Context(), id); err != nil {
				adminUserError(w, err)
				return
			}
		}
		u, err := store.UserByID(r.Context(), id)
		if err != nil {
			adminUserError(w, err)
			return
		}
		respond(w, http.StatusOK, api.AdminUserResponse{User: api.NewAdminUser(u)})
	}
}

// deleteUser godoc
// @Summary Delete a user
// @Description Removes the user and everything it owns.
// @Tags Admin
// @Produce json
// @Security AdminKey
// @Param id path string true "User UUID" format(uuid)
// @Success 204 "Deleted"
// @Failure 400,401,404,503,500 {object} map[string]APIError
// @Router /admin/users/{id} [delete]
func deleteUser(store UsersAdminStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := userID(w, r)
		if !ok {
			return
		}
		if err := store.DeleteUser(r.Context(), id); err != nil {
			adminUserError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
