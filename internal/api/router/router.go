package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/MeshCore-Tower/tower-server/internal/api"
	"github.com/MeshCore-Tower/tower-server/internal/api/handlers"
	mw "github.com/MeshCore-Tower/tower-server/internal/api/middleware"
	"github.com/MeshCore-Tower/tower-server/internal/hub"
	"github.com/MeshCore-Tower/tower-server/internal/ws"
)

// New builds and returns the top-level Chi router.
//
// Route shape:
//
//	/ws                → WebSocket (public in v1)
//	/api/v1/           → public group
//	  /packets         → packets subrouter
//	  /nodes           → nodes subrouter
//	  /observers       → observers subrouter
//	  /channels        → channels subrouter
//	  /iatas           → iatas subrouter
//	  /regions         → regions subrouter
//	  /stats           → stats subrouter
//	  /map             → map state subrouter
//
// The private group is stubbed and ready for the auth middleware drop-in
// described in Future Features → Admin authentication.
func New(h *hub.Hub, reader api.Reader) http.Handler {
	r := chi.NewRouter()

	// ── Global middleware ────────────────────────────────────────────────────
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.CleanPath)
	r.Use(middleware.StripSlashes)

	// ── WebSocket ────────────────────────────────────────────────────────────
	r.Get("/ws", ws.Handler(h))

	// ── Public REST API (v1) ─────────────────────────────────────────────────
	r.Route("/api/v1", func(r chi.Router) {
		// Public group — no authentication required (all of v1 is public).
		r.Group(func(r chi.Router) {
			r.Mount("/packets", handlers.PacketsRouter())
			r.Mount("/nodes", handlers.NodesRouter())
			r.Mount("/observers", handlers.ObserversRouter())
			r.Mount("/channels", handlers.ChannelsRouter())
			r.Mount("/iatas", handlers.IATAsRouter(reader))
			r.Mount("/regions", handlers.RegionsRouter(reader))
			r.Mount("/stats", handlers.StatsRouter())
			r.Mount("/map", handlers.MapRouter(reader))
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
