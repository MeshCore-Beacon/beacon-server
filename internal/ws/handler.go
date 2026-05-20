// Package ws handles the WebSocket endpoint at GET /ws.
//
// Protocol (from design doc):
//
//	On connect: server sends hello { v:1, type:"hello", serverTime:<ms>, connectionId:"uuid" }
//
//	Client → Server:
//	  subscribe   { v, type, id, scope }   → server replies subscribed { v, type, id, subscriptionId }
//	  unsubscribe { v, type, id, subscriptionId }
//	  ping        { v, type, id }          → server replies pong { v, type, id }
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
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"

	"tower/internal/hub"
)

const (
	pingTimeout  = 90 * time.Second
	writeTimeout = 10 * time.Second
)

// Handler returns an http.HandlerFunc that requires the hub to be injected.
// Wire it via router.New(h) so the hub is available at startup.
func Handler(h *hub.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// TODO: upgrade to WebSocket using nhooyr.io/websocket or gorilla/websocket.
		// The structure below shows the intended shape; swap the stub conn calls
		// for real ones once the library is chosen.

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
		log.Printf("ws[%s]: connected, hello: %s", connID, helloBytes)
		// TODO: conn.Write(ctx, websocket.MessageText, helloBytes)

		// Write pump: forward hub events to the WS connection.
		go func() {
			for {
				select {
				case evt, ok := <-client.Send:
					if !ok {
						return // hub closed the channel (client removed)
					}
					msg := map[string]any{
						"v":     1,
						"type":  "event",
						"event": evt.Type,
						"data":  json.RawMessage(evt.Payload),
					}
					msgBytes, _ := json.Marshal(msg)
					_ = msgBytes
					// TODO: conn.Write(ctx, websocket.MessageText, msgBytes)
				case <-ctx.Done():
					return
				}
			}
		}()

		// Read pump: handle subscribe / unsubscribe / ping from the client.
		// Drives the idle timeout; any message resets the deadline.
		for {
			// TODO: _, msgBytes, err := conn.Read(ctx)
			// if err != nil { return }
			// w.handleClientMessage(ctx, client, h, connID, msgBytes)

			// Stub: block until context cancelled.
			select {
			case <-ctx.Done():
				return
			case <-time.After(pingTimeout):
				log.Printf("ws[%s]: idle timeout", connID)
				return
			}
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
}

// subscribeScope mirrors the scope object in the subscribe message.
type subscribeScope struct {
	IATAs         []string        `json:"iatas"`
	RegionIDs     []string        `json:"regionIds"`
	PayloadTypes  []uint8         `json:"payloadTypes"`
	RouteTypes    []uint8         `json:"routeTypes"`
	ChannelHashes []string        `json:"channelHashes"`
	ObserverIDs   []string        `json:"observerIds"`
	Events        []hub.EventType `json:"events"`
}

// handleClientMessage dispatches a parsed client message.
// TODO: call this from the read pump once the WS library is wired.
func handleClientMessage(ctx context.Context, client *hub.Client, h *hub.Hub, connID string, raw []byte) {
	var msg clientMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		log.Printf("ws[%s]: bad message: %v", connID, err)
		return
	}

	switch msg.Type {
	case "subscribe":
		if msg.Scope == nil {
			return
		}
		scope := hub.Scope{
			IATAs:         msg.Scope.IATAs,
			PayloadTypes:  msg.Scope.PayloadTypes,
			ChannelHashes: msg.Scope.ChannelHashes,
			Events:        msg.Scope.Events,
			// RegionIATAs: TODO expand msg.Scope.RegionIDs → IATA list via DB/config lookup
		}
		h.AddScope(client, scope)
		subID := uuid.NewString()
		reply, _ := json.Marshal(map[string]any{
			"v": 1, "type": "subscribed", "id": msg.ID, "subscriptionId": subID,
		})
		log.Printf("ws[%s]: subscribed %s → %s", connID, msg.ID, subID)
		_ = reply
		// TODO: conn.Write(ctx, websocket.MessageText, reply)

	case "unsubscribe":
		// TODO: remove the specific subscriptionId from client.scope.
		// For now scope entries are append-only; implement removal when needed.
		log.Printf("ws[%s]: unsubscribe %s (TODO)", connID, msg.SubscriptionID)

	case "ping":
		reply, _ := json.Marshal(map[string]any{"v": 1, "type": "pong", "id": msg.ID})
		_ = reply
		// TODO: conn.Write(ctx, websocket.MessageText, reply)

	default:
		log.Printf("ws[%s]: unknown message type %q", connID, msg.Type)
	}

	_ = ctx
}
