// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// PollingAdminStore is what the polling admin routes read and write.
type PollingAdminStore interface {
	PollingState(ctx context.Context) (paused bool, pausedUntil *time.Time, excluded [][]byte, err error)
	SetPollingState(ctx context.Context, paused bool, pausedUntil *time.Time, excluded [][]byte) error
	ListPollingObservers(ctx context.Context) ([]api.PollingObserver, error)
	SetPollingObserver(ctx context.Context, observer []byte, blocked, approved bool, note string) error
}

// PollingAdminRouter must be mounted inside the authenticated admin subtree.
func PollingAdminRouter(store PollingAdminStore) http.Handler {
	if store == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { respondError(w, 503, "polling is unavailable") })
	}
	r := chi.NewRouter()
	r.Get("/", getPollingAdmin(store))
	r.Put("/state", putPollingState(store))
	r.Put("/observers/{publicKey}", putPollingObserver(store))
	return r
}

// decodePollingBody reads one JSON object of at most 4 KiB; it answers the error itself and reports false.
func decodePollingBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		respondError(w, 415, "Content-Type must be application/json")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err = dec.Decode(dst)
	if err == nil {
		var extra any
		if err = dec.Decode(&extra); errors.Is(err, io.EOF) {
			err = nil
		} else if err == nil {
			err = errors.New("multiple JSON values")
		}
	}
	if err != nil {
		respondError(w, 400, "invalid polling JSON")
		return false
	}
	return true
}

func pollingState(ctx context.Context, store PollingAdminStore) (api.PollingAdminState, error) {
	paused, until, excluded, err := store.PollingState(ctx)
	if err != nil {
		return api.PollingAdminState{}, err
	}
	observers, err := store.ListPollingObservers(ctx)
	if err != nil {
		return api.PollingAdminState{}, err
	}
	if observers == nil {
		observers = []api.PollingObserver{}
	}
	out := api.PollingAdminState{Paused: paused, ExcludedNodes: make([]string, 0, len(excluded)), Observers: observers}
	if until != nil {
		ms := until.UnixMilli()
		out.PausedUntil = &ms
	}
	for _, k := range excluded {
		out.ExcludedNodes = append(out.ExcludedNodes, hex.EncodeToString(k))
	}
	return out, nil
}

func pollingFailed(w http.ResponseWriter, err error) {
	slog.Error("polling admin request failed", "component", "api", "error", err)
	respondError(w, 500, "internal server error")
}

// getPollingAdmin godoc
// @Summary Get the repeater polling settings
// @Description Pause state, excluded repeaters and every observer the operator has blocked or approved.
// @Tags Admin
// @Produce json
// @Security AdminKey
// @Success 200 {object} api.PollingAdminState
// @Failure 401,503,500 {object} map[string]APIError
// @Router /admin/polling [get]
func getPollingAdmin(store PollingAdminStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		st, err := pollingState(r.Context(), store)
		if err != nil {
			pollingFailed(w, err)
			return
		}
		respond(w, 200, st)
	}
}

// putPollingState godoc
// @Summary Replace the polling pause and exclusions
// @Description Replaces the whole state. At most 16 excluded repeaters, each 64 hex characters; pausedUntil is epoch ms in the future or null. Body at most 4 KiB.
// @Tags Admin
// @Accept json
// @Produce json
// @Security AdminKey
// @Param state body api.PollingStateRequest true "New state"
// @Success 200 {object} api.PollingAdminState
// @Failure 400,401,415,503,500 {object} map[string]APIError
// @Router /admin/polling/state [put]
func putPollingState(store PollingAdminStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req api.PollingStateRequest
		if !decodePollingBody(w, r, &req) {
			return
		}
		until, nodes, err := req.Parse(time.Now())
		if err != nil {
			respondError(w, 400, err.Error())
			return
		}
		if err := store.SetPollingState(r.Context(), req.Paused, until, nodes); err != nil {
			pollingFailed(w, err)
			return
		}
		st, err := pollingState(r.Context(), store)
		if err != nil {
			pollingFailed(w, err)
			return
		}
		respond(w, 200, st)
	}
}

// putPollingObserver godoc
// @Summary Block or approve a polling observer
// @Description Replaces the observer's flags. The key is 64 hex characters in either case; the note is at most 200 characters. Body at most 4 KiB.
// @Tags Admin
// @Accept json
// @Produce json
// @Security AdminKey
// @Param publicKey path string true "Observer public key, 64 hex characters"
// @Param observer body api.PollingObserverRequest true "New flags"
// @Success 200 {object} api.PollingObserver
// @Failure 400,401,415,503,500 {object} map[string]APIError
// @Router /admin/polling/observers/{publicKey} [put]
func putPollingObserver(store PollingAdminStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, err := api.ParsePollingKey(chi.URLParam(r, "publicKey"))
		if err != nil {
			respondError(w, 400, "invalid observer public key")
			return
		}
		var req api.PollingObserverRequest
		if !decodePollingBody(w, r, &req) {
			return
		}
		if err := req.Validate(); err != nil {
			respondError(w, 400, err.Error())
			return
		}
		if err := store.SetPollingObserver(r.Context(), key, req.Blocked, req.Approved, req.Note); err != nil {
			pollingFailed(w, err)
			return
		}
		respond(w, 200, api.PollingObserver{PublicKey: hex.EncodeToString(key), Blocked: req.Blocked, Approved: req.Approved, Note: req.Note})
	}
}
