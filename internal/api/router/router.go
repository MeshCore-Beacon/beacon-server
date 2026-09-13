// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package router wires all HTTP routes onto the Chi router and injects
// dependencies (hub, reader, ingest workers) into the handler closures.
// REST routes are mounted under /api/v1; its admin subtree requires a bearer key.
package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	mw "github.com/MeshCore-Beacon/beacon-server/internal/api/middleware"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
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
// Admin handlers are added separately; the reserved subtree is protected now.
func New(h *hub.Hub, reader api.Reader, workers []*ingest.Worker, maxConnsPerIP int, corsCfg config.CORSConfig, serverCfg config.ServerConfig, authCfg config.AuthConfig) http.Handler {
	r := chi.NewRouter()

	// ── CORS ─────────────────────────────────────────────────────────────────
	allowedOrigins := corsCfg.AllowedOrigins
	if len(allowedOrigins) == 0 {
		allowedOrigins = []string{"*"}
	}
	allowedMethods := corsCfg.AllowedMethods
	if len(allowedMethods) == 0 {
		allowedMethods = []string{"GET", "HEAD", "OPTIONS"}
	}
	allowedHeaders := corsCfg.AllowedHeaders
	if len(allowedHeaders) == 0 {
		allowedHeaders = []string{"Accept", "Authorization", "Content-Type"}
	}
	maxAge := corsCfg.MaxAge
	if maxAge == 0 {
		maxAge = 300
	}
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   allowedOrigins,
		AllowedMethods:   allowedMethods,
		AllowedHeaders:   allowedHeaders,
		AllowCredentials: corsCfg.AllowCredentials,
		MaxAge:           maxAge,
	}))

	// ── Global middleware ────────────────────────────────────────────────────
	r.Use(middleware.RequestID)
	r.Use(mw.TrustedProxyIP(serverCfg.TrustedProxies))
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
		// Public group — no authentication required.
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

		// Protect the entire subtree, including its root and unknown paths.
		// Replace the empty router with the admin handlers when they are added.
		r.Mount("/admin", mw.BearerAuth(authCfg.APIKey, chi.NewRouter()))
	})

	return r
}
