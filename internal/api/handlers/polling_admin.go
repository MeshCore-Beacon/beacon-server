// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/go-chi/chi/v5"
)

// PollingAdminStore is what the polling admin routes read and write.
type PollingAdminStore interface {
	PollingState(ctx context.Context) (paused bool, pausedUntil *time.Time, excluded [][]byte, err error)
	SetPollingState(ctx context.Context, paused bool, pausedUntil *time.Time, excluded [][]byte) error
	ListPollingObservers(ctx context.Context) ([]api.PollingObserver, error)
	SetPollingObserver(ctx context.Context, observer []byte, blocked, approved bool, note string) error
	PollingTargetsAdmin(ctx context.Context, receivedAfter time.Time, limit int) ([]api.PollingTarget, error)
	ListPollingAdminStops(ctx context.Context) ([]api.PollingAdminStop, error)
	ListNoPasswordBlocked(ctx context.Context) ([]string, error)
	DeletePollingAdminStop(ctx context.Context, target, observer []byte) (bool, error)
	ClearNoPasswordBlocked(ctx context.Context, target []byte) (bool, error)
}

// PollingAdminConfig is what the admin routes show and check from the polling config.
type PollingAdminConfig struct {
	Paused        bool     // polling.paused
	Authority     string   // polling.authority
	ExcludedNodes [][]byte // polling.excluded_nodes
}

// PollingAdminRouter must be mounted inside the authenticated admin subtree.
func PollingAdminRouter(store PollingAdminStore, cfg PollingAdminConfig) http.Handler {
	if store == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { respondError(w, 503, "polling is unavailable") })
	}
	a := pollingAdmin{store: store, cfg: cfg}
	r := chi.NewRouter()
	r.Get("/", a.get)
	r.Get("/targets", getPollingTargets(store))
	r.Put("/state", a.putState)
	r.Put("/observers/{publicKey}", putPollingObserver(store))
	r.Put("/targets/{target}", putPollingTarget(store))
	r.Delete("/admin-stops/{target}/{observer}", deletePollingAdminStop(store))
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

type pollingAdmin struct {
	store PollingAdminStore
	cfg   PollingAdminConfig
}

func (a pollingAdmin) state(ctx context.Context) (api.PollingAdminState, error) {
	paused, until, excluded, err := a.store.PollingState(ctx)
	if err != nil {
		return api.PollingAdminState{}, err
	}
	observers, err := a.store.ListPollingObservers(ctx)
	if err != nil {
		return api.PollingAdminState{}, err
	}
	if observers == nil {
		observers = []api.PollingObserver{}
	}
	// A pause whose end has passed no longer pauses anything, as the policy answer already treats it.
	effective := paused && (until == nil || until.After(time.Now()))
	blocked, err := a.store.ListNoPasswordBlocked(ctx)
	if err != nil {
		return api.PollingAdminState{}, err
	}
	stops, err := a.store.ListPollingAdminStops(ctx)
	if err != nil {
		return api.PollingAdminState{}, err
	}
	if blocked == nil {
		blocked = []string{}
	}
	if stops == nil {
		stops = []api.PollingAdminStop{}
	}
	out := api.PollingAdminState{NoPasswordBlocked: blocked, AdminStops: stops, Paused: effective, ConfigPaused: a.cfg.Paused, Authority: a.cfg.Authority,
		ExcludedNodes: make([]string, 0, len(excluded)), ConfigExcludedNodes: make([]string, 0, len(a.cfg.ExcludedNodes)),
		Observers: observers}
	for _, k := range a.cfg.ExcludedNodes {
		out.ConfigExcludedNodes = append(out.ConfigExcludedNodes, hex.EncodeToString(k))
	}
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
// @Description Pause state, excluded repeaters and every observer the operator has blocked or approved. paused is false once pausedUntil has passed; configPaused, configExcludedNodes and authority come from config. The policy excludes the union of excludedNodes and configExcludedNodes.
// @Tags Admin
// @Produce json
// @Security AdminKey
// @Success 200 {object} api.PollingAdminState
// @Failure 401,503,500 {object} map[string]APIError
// @Router /admin/polling [get]
func (a pollingAdmin) get(w http.ResponseWriter, r *http.Request) {
	st, err := a.state(r.Context())
	if err != nil {
		pollingFailed(w, err)
		return
	}
	respond(w, 200, st)
}

// getPollingTargets godoc
// @Summary List polled repeaters and their observers
// @Description Every repeater with a live claim or an assignment, newest claim first, at most 500. Each lists its live candidates and assigned observers; rank is null for an unassigned candidate. latestTrustedReading counts only readings from an assigned observer with advert evidence; approved is the operator's approval of the observer.
// @Tags Admin
// @Produce json
// @Security AdminKey
// @Success 200 {object} api.PollingTargetList
// @Failure 401,503,500 {object} map[string]APIError
// @Router /admin/polling/targets [get]
func getPollingTargets(store PollingAdminStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		targets, err := store.PollingTargetsAdmin(r.Context(), time.Now().Add(-polling.ClaimTTL), api.MaxPollingTargets)
		if err != nil {
			pollingFailed(w, err)
			return
		}
		if targets == nil {
			targets = []api.PollingTarget{}
		}
		respond(w, 200, api.PollingTargetList{Targets: targets})
	}
}

// putPollingState godoc
// @Summary Replace the polling pause and exclusions
// @Description Replaces the whole state. Excluded repeaters are 64 hex characters each; together with polling.excluded_nodes from config they may name at most 16 distinct repeaters, otherwise 400 and nothing changes. pausedUntil is epoch ms in the future or null. Body at most 4 KiB.
// @Tags Admin
// @Accept json
// @Produce json
// @Security AdminKey
// @Param state body api.PollingStateRequest true "New state"
// @Success 200 {object} api.PollingAdminState
// @Failure 400,401,415,503,500 {object} map[string]APIError
// @Router /admin/polling/state [put]
func (a pollingAdmin) putState(w http.ResponseWriter, r *http.Request) {
	var req api.PollingStateRequest
	if !decodePollingBody(w, r, &req) {
		return
	}
	until, nodes, err := req.Parse(time.Now())
	if err != nil {
		respondError(w, 400, err.Error())
		return
	}
	// The policy carries the union of both lists; refuse what would not fit rather than drop it there.
	union := map[string]bool{}
	for _, k := range a.cfg.ExcludedNodes {
		union[string(k)] = true
	}
	for _, k := range nodes {
		union[string(k)] = true
	}
	if len(union) > api.MaxPollingExcludedNodes {
		respondError(w, 400, fmt.Sprintf("excludedNodes: %d keys with the %d in config make %d distinct; at most %d in total",
			len(nodes), len(a.cfg.ExcludedNodes), len(union), api.MaxPollingExcludedNodes))
		return
	}
	if err := a.store.SetPollingState(r.Context(), req.Paused, until, nodes); err != nil {
		pollingFailed(w, err)
		return
	}
	st, err := a.state(r.Context())
	if err != nil {
		pollingFailed(w, err)
		return
	}
	respond(w, 200, st)
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

// putPollingTarget godoc
// @Summary Lift the blank-login block on a requested repeater
// @Description Only noPasswordBlocked false is accepted. A repeater that gave admin access to a blank login stays blocked until this is called; requests that were stopped do not restart by themselves. 404 for a repeater Beacon has no record of. Body at most 4 KiB.
// @Tags Admin
// @Accept json
// @Produce json
// @Security AdminKey
// @Param target path string true "Repeater public key, 64 hex characters"
// @Param body body api.PollingTargetUpdate true "noPasswordBlocked: false"
// @Success 200 {object} api.PollingTargetUpdate
// @Failure 400,401,404,415,503,500 {object} map[string]APIError
// @Router /admin/polling/targets/{target} [put]
func putPollingTarget(store PollingAdminStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key, err := api.ParsePollingKey(chi.URLParam(r, "target"))
		if err != nil {
			respondError(w, 400, "invalid repeater public key")
			return
		}
		var req api.PollingTargetUpdate
		if !decodePollingBody(w, r, &req) {
			return
		}
		if req.NoPasswordBlocked {
			respondError(w, 400, "noPasswordBlocked can only be set to false")
			return
		}
		found, err := store.ClearNoPasswordBlocked(r.Context(), key)
		if err != nil {
			pollingFailed(w, err)
			return
		}
		if !found {
			respondError(w, 404, "no such repeater")
			return
		}
		respond(w, 200, req)
	}
}

// deletePollingAdminStop godoc
// @Summary Clear one admin stop
// @Description The observer may be offered the requested repeater again at the next election. Its device keeps its own stop until the operator runs poll enable on it. 404 when there is no such stop.
// @Tags Admin
// @Security AdminKey
// @Param target path string true "Repeater public key, 64 hex characters"
// @Param observer path string true "Observer public key, 64 hex characters"
// @Success 204 "Cleared"
// @Failure 400,401,404,503,500 {object} map[string]APIError
// @Router /admin/polling/admin-stops/{target}/{observer} [delete]
func deletePollingAdminStop(store PollingAdminStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		target, err := api.ParsePollingKey(chi.URLParam(r, "target"))
		if err != nil {
			respondError(w, 400, "invalid repeater public key")
			return
		}
		observer, err := api.ParsePollingKey(chi.URLParam(r, "observer"))
		if err != nil {
			respondError(w, 400, "invalid observer public key")
			return
		}
		found, err := store.DeletePollingAdminStop(r.Context(), target, observer)
		if err != nil {
			pollingFailed(w, err)
			return
		}
		if !found {
			respondError(w, 404, "no such admin stop")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
