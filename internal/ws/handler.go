// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package ws handles the WebSocket endpoint at GET /ws.
//
// Protocol (from design doc):
//
//	On connect: server sends hello { v:1, type:"hello", serverTime:<ms>, connectionId:"uuid" }
//
//	Client → Server:
//	  subscribe   { v, type, id, scope }         → server replies subscribed { v, type, id, subscriptionId }
//	  unsubscribe { v, type, id, subscriptionId }
//	  configure   { v, type, id, resolvePath }   → server replies configured { v, type, id, resolvePath }
//	  ping        { v, type, id }                → server replies pong { v, type, id }
//
//	configure's resolvePath (bool) is a connection-wide setting, not
//	per-subscription: enables/disables per-hop resolvedPath data on
//	packetObservation events. Freely toggleable at any point during the
//	connection; each configure call sets it to exactly the value sent
//	(not additive across calls, unlike subscribe scopes). Default false.
//
//	Server → Client events (unsolicited):
//	  packetObservation, observerStatus, nodeUpdate, channelMessage
//	  lagged { v, type, droppedCount, since, lastObservationId }
//	  error  { v, type, code, message }
//
//	Idle connections (no ping) closed after 90s.
//	Client should ping every 30s.
package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
)

const (
	pingTimeout  = 90 * time.Second // server closes connection if no message received within this window
	writeTimeout = 10 * time.Second // TODO: apply per-write deadline once nhooyr supports it cleanly
)

// Handler returns an http.HandlerFunc that requires the hub to be injected.
// Wire it via router.New(h) so the hub is available at startup.
func Handler(h *hub.Hub, reader api.Reader, maxConnsPerIP int) http.HandlerFunc {
	limiter := newIPLimiter(maxConnsPerIP)
	return func(w http.ResponseWriter, r *http.Request) {
		ip := r.RemoteAddr
		if host, _, err := net.SplitHostPort(ip); err == nil {
			ip = host
		}
		if !limiter.acquire(ip) {
			slog.Warn(fmt.Sprintf("ws: connection limit reached for IP %s", ip), "component", "ws")
			http.Error(w, "too many connections from this IP", http.StatusTooManyRequests)
			return
		}
		defer limiter.release(ip)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			slog.Warn("ws: failed to accept connection", "component", "ws", "error", err)
			return
		}

		connID := uuid.NewString()
		client := h.NewClient()
		defer h.Remove(client)

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		// Send hello
		hello := map[string]any{
			"v":            1,
			"type":         "hello",
			"serverTime":   time.Now().UnixMilli(),
			"connectionId": connID,
		}
		helloBytes, _ := json.Marshal(hello)
		slog.Debug("connected", "component", "ws", "connection_id", connID)
		err = conn.Write(ctx, websocket.MessageText, helloBytes)
		if err != nil {
			slog.Warn(fmt.Sprintf("ws[%s]: failed to send hello", connID), "component", "ws", "error", err)
			return
		}

		// Write pump: forward hub events to the WS connection.
		go func() {
			for {
				select {
				case evt, ok := <-client.Send:
					if !ok {
						return
					}
					msg := map[string]any{
						"v":     1,
						"type":  "event",
						"event": evt.Type,
						"data":  json.RawMessage(evt.Payload),
					}
					msgBytes, _ := json.Marshal(msg)
					err = conn.Write(ctx, websocket.MessageText, msgBytes)
					if err != nil {
						slog.Warn(fmt.Sprintf("ws[%s]: failed to write hub event", connID), "component", "ws", "error", err)
						cancel()
						return
					}

				case lag, ok := <-client.LaggedCH():
					if !ok {
						return
					}
					lagged := map[string]any{
						"v":            1,
						"type":         "lagged",
						"droppedCount": lag.DroppedCount,
						"since":        time.Now().UnixMilli(),
					}
					lagBytes, _ := json.Marshal(lagged)
					if err := conn.Write(ctx, websocket.MessageText, lagBytes); err != nil {
						slog.Warn(fmt.Sprintf("ws[%s]: failed to write lagged notice", connID), "component", "ws", "error", err)
						cancel()
						return
					}

				case <-ctx.Done():
					return
				}
			}
		}()

		// Read pump: handle subscribe / unsubscribe / ping from the client.
		// Drives the idle timeout; any message resets the deadline.
		for {
			readCtx, readCancel := context.WithTimeout(ctx, pingTimeout)
			_, msgBytes, err := conn.Read(readCtx)
			readCancel()
			if err != nil {
				slog.Warn(fmt.Sprintf("ws[%s]: read error", connID), "component", "ws", "error", err)
				return
			}
			handleClientMessage(ctx, client, reader, h, conn, connID, msgBytes)
		}
	}
}

// clientMessage is the shape of every client → server message.
type clientMessage struct {
	V              int             `json:"v"`
	Type           string          `json:"type"`
	ID             string          `json:"id"`
	SubscriptionID string          `json:"subscriptionId,omitempty"`
	Scope          *subscribeScope `json:"scope,omitempty"`

	// ResolvePath is only read for "configure" messages: enables/disables
	// the resolvedPath variant of packetObservation events for the whole
	// connection. See package doc.
	ResolvePath bool `json:"resolvePath,omitempty"`
}

// subscribeScope mirrors the scope object in the subscribe message.
type subscribeScope struct {
	IATAs         []string        `json:"iatas"`
	RegionIDs     []string        `json:"regionIds"`
	RegionSlugs   []string        `json:"regionSlugs"`
	PayloadTypes  []uint8         `json:"payloadTypes"`
	RouteTypes    []uint8         `json:"routeTypes"`
	ChannelHashes []string        `json:"channelHashes"`
	ObserverIDs   []string        `json:"observerIds"`
	Events        []hub.EventType `json:"events"`
}

// handleClientMessage dispatches a parsed client message.
func handleClientMessage(ctx context.Context, client *hub.Client, reader api.Reader, h *hub.Hub, conn *websocket.Conn, connID string, raw []byte) {
	var msg clientMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		slog.Warn("bad message", "component", "ws", "connection_id", connID, "error", err)
		return
	}

	switch msg.Type {
	case "subscribe":
		if msg.Scope == nil {
			return
		}
		iatas := msg.Scope.IATAs
		for _, ridStr := range msg.Scope.RegionIDs {
			rid, err := strconv.ParseInt(ridStr, 10, 32)
			if err != nil {
				slog.Warn(fmt.Sprintf("ws[%s]: invalid regionId %q, skipping", connID, ridStr), "component", "ws")
				continue
			}
			region, err := reader.GetRegion(ctx, int32(rid))
			if err != nil {
				slog.Warn(fmt.Sprintf("ws[%s]: region %d not found, skipping", connID, rid), "component", "ws", "error", err)
				continue
			}
			iatas = append(iatas, region.IATAs...)
		}
		for _, slug := range msg.Scope.RegionSlugs {
			region, err := reader.GetRegionBySlug(ctx, slug)
			if err != nil {
				slog.Warn(fmt.Sprintf("ws[%s]: region slug %q not found, skipping", connID, slug), "component", "ws", "error", err)
				continue
			}
			iatas = append(iatas, region.IATAs...)
		}
		scope := hub.Scope{
			IATAs:         iatas,
			PayloadTypes:  msg.Scope.PayloadTypes,
			ChannelHashes: msg.Scope.ChannelHashes,
			Events:        msg.Scope.Events,
		}
		subID := uuid.NewString()
		h.AddScope(client, subID, scope)
		reply, _ := json.Marshal(map[string]any{
			"v": 1, "type": "subscribed", "id": msg.ID, "subscriptionId": subID,
		})
		if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
			slog.Debug(fmt.Sprintf("ws[%s]: subscribed %s → %s", connID, msg.ID, subID), "component", "ws")
		}
		err := conn.Write(ctx, websocket.MessageText, reply)
		if err != nil {
			slog.Warn("failed to send subscribed reply", "component", "ws", "connection_id", connID, "error", err)
		}

	case "unsubscribe":
		if msg.SubscriptionID == "" {
			return
		}
		h.RemoveScope(client, msg.SubscriptionID)
		reply, _ := json.Marshal(map[string]any{
			"v": 1, "type": "unsubscribed", "id": msg.ID, "subscriptionId": msg.SubscriptionID,
		})
		if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
			slog.Debug(fmt.Sprintf("ws[%s]: unsubscribed %s", connID, msg.SubscriptionID), "component", "ws")
		}
		if err := conn.Write(ctx, websocket.MessageText, reply); err != nil {
			slog.Warn("failed to send unsubscribed reply", "component", "ws", "connection_id", connID, "error", err)
		}

	case "configure":
		h.SetResolvePath(client, msg.ResolvePath)
		reply, _ := json.Marshal(map[string]any{
			"v": 1, "type": "configured", "id": msg.ID, "resolvePath": msg.ResolvePath,
		})
		if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
			slog.Debug(fmt.Sprintf("ws[%s]: configured resolvePath=%t", connID, msg.ResolvePath), "component", "ws")
		}
		if err := conn.Write(ctx, websocket.MessageText, reply); err != nil {
			slog.Warn(fmt.Sprintf("ws[%s]: failed to send configured reply", connID), "component", "ws", "error", err)
		}

	case "ping":
		reply, _ := json.Marshal(map[string]any{"v": 1, "type": "pong", "id": msg.ID})
		err := conn.Write(ctx, websocket.MessageText, reply)
		if err != nil {
			slog.Warn(fmt.Sprintf("ws[%s]: failed to send pong", connID), "component", "ws", "error", err)
		}

	default:
		slog.Warn("unknown message type", "component", "ws", "connection_id", connID)
	}
}
