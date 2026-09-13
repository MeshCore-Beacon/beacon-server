// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// AccountsRouter must be mounted inside the authenticated admin subtree.
func AccountsRouter(store api.AccountStore) http.Handler {
	if store == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { respondError(w, 503, "account storage is unavailable") })
	}
	r := chi.NewRouter()
	r.Get("/", listAccounts(store))
	r.Post("/", createAccount(store))
	r.Get("/{id}", getAccount(store))
	r.Delete("/{id}", deactivateAccount(store))
	return r
}

// listAccounts godoc
// @Summary List operator accounts
// @Description Includes active and inactive accounts, newest first. These records are not login users.
// @Tags Admin
// @Produce json
// @Security AdminKey
// @Success 200 {object} api.AccountList
// @Failure 401,503,500 {object} map[string]APIError
// @Router /admin/accounts [get]
func listAccounts(store api.AccountStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		items, err := store.ListAccounts(r.Context())
		if err != nil {
			accountError(w, err)
			return
		}
		if items == nil {
			items = []api.Account{}
		}
		respond(w, 200, api.AccountList{Items: items})
	}
}

// createAccount godoc
// @Summary Create an operator account
// @Description Trims the name; names are case-sensitive and unique among active accounts. No credential or login is created.
// @Tags Admin
// @Accept json
// @Produce json
// @Security AdminKey
// @Param account body api.CreateAccountRequest true "Account name (1-128 Unicode characters, no control characters); body at most 4 KiB"
// @Success 201 {object} api.Account
// @Failure 400,401,409,413,415,503,500 {object} map[string]APIError
// @Router /admin/accounts [post]
func createAccount(store api.AccountStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			respondError(w, 415, "Content-Type must be application/json")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var input api.CreateAccountRequest
		err = decoder.Decode(&input)
		if err == nil {
			var extra any
			err = decoder.Decode(&extra)
			if errors.Is(err, io.EOF) {
				err = nil
			} else if err == nil {
				err = errors.New("multiple JSON values")
			}
		}
		if err != nil {
			var sizeError *http.MaxBytesError
			if errors.As(err, &sizeError) {
				respondError(w, 413, "request body exceeds 4 KiB")
			} else {
				respondError(w, 400, "invalid account JSON")
			}
			return
		}
		name, err := api.NormalizeAccountName(input.Name)
		if err != nil {
			accountError(w, err)
			return
		}
		account, err := store.CreateAccount(r.Context(), name)
		if err != nil {
			accountError(w, err)
			return
		}
		respond(w, 201, account)
	}
}

// getAccount godoc
// @Summary Get an operator account
// @Description Returns active or inactive account records.
// @Tags Admin
// @Produce json
// @Security AdminKey
// @Param id path string true "Account UUID" format(uuid)
// @Success 200 {object} api.Account
// @Failure 400,401,404,503,500 {object} map[string]APIError
// @Router /admin/accounts/{id} [get]
func getAccount(store api.AccountStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			respondError(w, 400, "invalid account ID")
			return
		}
		account, err := store.GetAccount(r.Context(), id)
		if err != nil {
			accountError(w, err)
			return
		}
		respond(w, 200, account)
	}
}

// deactivateAccount godoc
// @Summary Deactivate an operator account
// @Description Soft deactivation preserves the record. Its name may be reused by a new account.
// @Tags Admin
// @Produce json
// @Security AdminKey
// @Param id path string true "Account UUID" format(uuid)
// @Success 204 "Deactivated"
// @Failure 400,401,404,409,503,500 {object} map[string]APIError
// @Router /admin/accounts/{id} [delete]
func deactivateAccount(store api.AccountStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			respondError(w, 400, "invalid account ID")
			return
		}
		if err := store.DeactivateAccount(r.Context(), id); err != nil {
			accountError(w, err)
			return
		}
		w.WriteHeader(204)
	}
}

func accountError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, api.ErrAccountNameInvalid):
		respondError(w, 400, api.ErrAccountNameInvalid.Error())
	case errors.Is(err, api.ErrAccountNameConflict):
		respondError(w, 409, api.ErrAccountNameConflict.Error())
	case errors.Is(err, api.ErrAccountNotFound):
		respondError(w, 404, api.ErrAccountNotFound.Error())
	case errors.Is(err, api.ErrAccountInactive):
		respondError(w, 409, api.ErrAccountInactive.Error())
	default:
		respondError(w, 500, "internal server error")
	}
}
