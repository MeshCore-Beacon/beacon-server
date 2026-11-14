// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
)

const (
	maxClientLen        = 32
	maxClientVersionLen = 23
)

// GuestPolling answers the observer firmware's policy and owners requests. An error means the state could not
// be read.
type GuestPolling interface {
	Policy(ctx context.Context, c polling.Caller) (polling.Policy, error)
	Owners(ctx context.Context, c polling.Caller) (polling.OwnersAnswer, error)
}

// GuestPollingRouter serves /owners and /policy. A nil svc answers the disabled policy.
func GuestPollingRouter(svc GuestPolling) http.Handler {
	r := chi.NewRouter()
	r.Get("/policy", func(w http.ResponseWriter, r *http.Request) {
		c := callerFromQuery(r)
		if svc == nil {
			writeFirmwareJSON(w, polling.DisabledPolicy())
			return
		}
		p, err := svc.Policy(r.Context(), c)
		if err != nil {
			firmwareUnavailable(w)
			return
		}
		writeFirmwareJSON(w, p)
	})
	r.Get("/owners", func(w http.ResponseWriter, r *http.Request) {
		c := callerFromQuery(r)
		if svc == nil {
			writeFirmwareJSON(w, polling.OwnersAnswer{Nodes: map[string]polling.OwnerEntry{}, Policy: polling.DisabledPolicy()})
			return
		}
		a, err := svc.Owners(r.Context(), c)
		if err != nil {
			firmwareUnavailable(w)
			return
		}
		writeFirmwareJSON(w, a)
	})
	return r
}

// GuestKeysGone answers the retired key routes with 404 and never touches the request body.
func GuestKeysGone() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		respondError(w, http.StatusNotFound, "not found")
	}
}

// callerFromQuery never fails: a bad observer value just leaves Observer nil.
func callerFromQuery(r *http.Request) polling.Caller {
	q := r.URL.Query()
	c := polling.Caller{
		Client:        capString(q.Get("client"), maxClientLen),
		ClientVersion: capString(q.Get("client_version"), maxClientVersionLen),
	}
	if v := q.Get("observer"); len(v) == 64 {
		if b, err := hex.DecodeString(v); err == nil {
			c.Observer = b
		}
	}
	return c
}

func capString(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// writeFirmwareJSON marshals first so Content-Length is exact and the body goes out in one write.
func writeFirmwareJSON(w http.ResponseWriter, v any) {
	writeFirmware(w, http.StatusOK, v)
}

// firmwareUnavailable answers 503: the firmware keeps its saved policy and roles on any non-200 but a 404.
func firmwareUnavailable(w http.ResponseWriter) {
	w.Header().Set("Retry-After", "60")
	writeFirmware(w, http.StatusServiceUnavailable,
		map[string]APIError{"error": {Code: "service_unavailable", Message: "polling state is unavailable"}})
}

func writeFirmware(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		slog.Error("api: failed to encode firmware response", "component", "api", "error", err)
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
