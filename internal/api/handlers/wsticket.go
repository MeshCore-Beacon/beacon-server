// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"net/http"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/wsticket"
	"github.com/go-chi/chi/v5"
)

// WSTicketRouter mounts GET /ws-ticket. It stays mounted whatever websocket.require_origin says,
// so the app can always ask.
func WSTicketRouter(tickets *wsticket.Issuer) http.Handler {
	r := chi.NewRouter()
	r.Get("/", getWSTicket(tickets))
	return r
}

// getWSTicket godoc
//
//	@Summary		Ticket for opening /ws without an Origin
//	@Description	For clients that send no Origin header, such as BEACON Mobile. Send the ticket in the
//	@Description	X-Beacon-Ticket header of the /ws upgrade. It works for 60 seconds, from the same client
//	@Description	address (IPv6: the same /64), and only on this server process. Required only when
//	@Description	websocket.require_origin is on; browsers never need one.
//	@Tags			Info
//	@Produce		json
//	@Success		200	{object}	api.WSTicket
//	@Router			/ws-ticket [get]
func getWSTicket(tickets *wsticket.Issuer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ticket, expires := tickets.Issue(wsticket.ClientKey(r))
		w.Header().Set("Cache-Control", "no-store")
		respond(w, http.StatusOK, api.WSTicket{Ticket: ticket, ExpiresAt: expires.UnixMilli()})
	}
}
