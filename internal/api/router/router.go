// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package router wires all HTTP routes onto the Chi router and injects
// dependencies (hub, reader, ingest workers) into the handler closures.
// All routes are mounted under /api/v1 with public and private groups
// stubbed for future auth middleware.
package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	mw "github.com/MeshCore-Beacon/beacon-server/internal/api/middleware"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/MeshCore-Beacon/beacon-server/internal/ws"
	httpSwagger "github.com/swaggo/http-swagger"
)

// New builds and returns the top-level Chi router.
//
// Route shape:
//
//	/ws                → WebSocket (public in v1)
//	/api/v1/           → public group
//	  /packets         → packets subrouter
//	  /nodes           → nodes subrouter
//	  /brokers         → brokers subrouter
//	  /observers       → observers subrouter
//	  /channels        → channels subrouter
//	  /iatas           → iatas subrouter
//	  /regions         → regions subrouter
//	  /stats           → stats subrouter
//
// The private group is stubbed and ready for the auth middleware drop-in
// described in Future Features → Admin authentication.
func New(h *hub.Hub, reader api.Reader, workers []*ingest.Worker, maxConnsPerIP int) http.Handler {
	r := chi.NewRouter()

	// ── Global middleware ────────────────────────────────────────────────────
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.CleanPath)
	r.Use(middleware.StripSlashes)

	// ── Swagger UI ──────────────────────────────────────────────────────────
	r.Get("/swagger", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/swagger/index.html", http.StatusMovedPermanently)
	})

	r.Get("/swagger/*", httpSwagger.Handler(
		httpSwagger.URL("/swagger/doc.json"),
	))

	// ── WebSocket ────────────────────────────────────────────────────────────
	r.Get("/ws", ws.Handler(h, reader, maxConnsPerIP))

	// ── Public REST API (v1) ─────────────────────────────────────────────────
	r.Route("/api/v1", func(r chi.Router) {
		// Public group — no authentication required (all of v1 is public).
		r.Group(func(r chi.Router) {
			r.Mount("/packets", handlers.PacketsRouter(reader))
			r.Mount("/nodes", handlers.NodesRouter(reader))
			r.Mount("/brokers", handlers.BrokersRouter(workers))
			r.Mount("/observers", handlers.ObserversRouter(reader))
			r.Mount("/channels", handlers.ChannelsRouter(reader))
			r.Mount("/messages", handlers.MessagesRouter(reader))
			r.Mount("/iatas", handlers.IATAsRouter(reader))
			r.Mount("/regions", handlers.RegionsRouter(reader))
			r.Mount("/routes", handlers.RoutesRouter(reader))
			r.Mount("/scopes", handlers.ScopesRouter(reader))
			r.Mount("/stats", handlers.StatsRouter(reader))
			r.Mount("/traces", handlers.TracesRouter(reader))
		})

		// Private group — auth middleware applied.
		// Stubbed for the admin endpoints described in Future Features.
		// Swap mw.NoopAuth for a real JWT/session middleware when ready.
		r.Group(func(r chi.Router) {
			r.Use(mw.NoopAuth)
			// r.Mount("/admin", handlers.AdminRouter())
		})
	})

	return r
}
