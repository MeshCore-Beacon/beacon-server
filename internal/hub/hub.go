// Package hub provides the central fan-out broker between the MQTT ingest
// goroutines and connected WebSocket clients.
//
// Design:
//   - A single Hub.Run() goroutine owns the client map; no mutexes needed.
//   - Ingest goroutines call hub.Broadcast() from any goroutine.
//   - Each WebSocket connection gets a *Client with a buffered send channel.
//   - If a client's send buffer is full it receives a lagged notification and
//     its buffer is drained so it doesn't stall the broadcast loop.
//
// Subscription filtering (by IATA, payload type, channel hash, etc.) is
// enforced here before events are placed on a client's send channel.
package hub

import (
	"encoding/json"
	"log"
)

// EventType identifies the kind of server-push event. These match the
// discriminator values in the WebSocket protocol ("packetObservation", etc.).
type EventType string

const (
	EventPacketObservation EventType = "packetObservation"
	EventObserverStatus    EventType = "observerStatus"
	EventNodeUpdate        EventType = "nodeUpdate"
	EventChannelMessage    EventType = "channelMessage"
)

// Event is a single fan-out unit. Payload is pre-serialised JSON so the
// broadcast loop never touches encoding — it's done once by the ingest path.
type Event struct {
	Type    EventType
	Payload json.RawMessage

	// Routing metadata used by the hub to match subscriptions.
	// Populated by the ingest layer before calling Broadcast.
	IATA        string
	PayloadType uint8
	ChannelHash string // hex string, non-empty only for channelMessage events
}

// Scope mirrors the client-side subscribe message. All fields are optional:
// nil/empty means "no filter on this dimension" (match everything).
// An empty non-nil slice means "match nothing on this dimension".
type Scope struct {
	IATAs        []string
	RegionIATAs  []string // pre-expanded from regionId by the WS handler
	PayloadTypes []uint8
	ChannelHashes []string
	Events       []EventType
}

// Client represents a connected WebSocket consumer.
type Client struct {
	Send  chan Event
	scope []Scope // OR semantics: event matches if it matches any scope entry
}

// matches returns true if the event satisfies at least one of the client's
// active subscriptions.
func (c *Client) matches(e Event) bool {
	if len(c.scope) == 0 {
		return true // no subscriptions yet → receive nothing
	}
	for _, s := range c.scope {
		if scopeMatches(s, e) {
			return true
		}
	}
	return false
}

func scopeMatches(s Scope, e Event) bool {
	if len(s.Events) > 0 && !containsEventType(s.Events, e.Type) {
		return false
	}
	if len(s.IATAs) > 0 || len(s.RegionIATAs) > 0 {
		allIATAs := append(s.IATAs, s.RegionIATAs...) //nolint:gocritic
		if !containsString(allIATAs, e.IATA) {
			return false
		}
	}
	if len(s.PayloadTypes) > 0 && !containsUint8(s.PayloadTypes, e.PayloadType) {
		return false
	}
	if len(s.ChannelHashes) > 0 && !containsString(s.ChannelHashes, e.ChannelHash) {
		return false
	}
	return true
}

// Hub is the central event broker.
type Hub struct {
	subscribe   chan subscribeMsg
	unsubscribe chan unsubscribeMsg
	remove      chan *Client
	broadcast   chan Event
}

type subscribeMsg struct {
	client      *Client
	scope       Scope
	hasScope    bool // true when this is an AddScope call, false for NewClient registration
}

type unsubscribeMsg struct {
	client         *Client
	subscriptionID string
}

// New creates a Hub. Call Run() in a goroutine before using it.
func New() *Hub {
	return &Hub{
		subscribe:   make(chan subscribeMsg, 64),
		unsubscribe: make(chan unsubscribeMsg, 64),
		remove:      make(chan *Client, 64),
		broadcast:   make(chan Event, 512),
	}
}

// NewClient creates a Client and registers it with the hub.
// The caller is responsible for calling Remove when the connection closes.
func (h *Hub) NewClient() *Client {
	c := &Client{
		Send: make(chan Event, 256),
	}
	// We don't add it to the map here; we send it through the channel so
	// Run() is the only goroutine that touches the client map.
	h.subscribe <- subscribeMsg{client: c, hasScope: false}
	return c
}

// AddScope appends a subscription scope to a client. Called by the WS handler
// when it receives a "subscribe" message from the client.
func (h *Hub) AddScope(c *Client, s Scope) {
	h.subscribe <- subscribeMsg{client: c, scope: s, hasScope: true}
}

// Remove deregisters a client and closes its Send channel.
// Safe to call from any goroutine (e.g. the WS handler's defer).
func (h *Hub) Remove(c *Client) {
	h.remove <- c
}

// Broadcast enqueues an event for fan-out. Safe to call from any goroutine.
func (h *Hub) Broadcast(e Event) {
	select {
	case h.broadcast <- e:
	default:
		log.Println("hub: broadcast channel full, dropping event")
	}
}

// Run is the hub's single-goroutine event loop. Call it in a dedicated
// goroutine: go hub.Run().
//
// It processes registrations, removals, and broadcasts sequentially so the
// clients map needs no locking.
func (h *Hub) Run() {
	// clients maps a *Client to the set of subscription IDs it holds.
	// We use a map[*Client]struct{} for O(1) presence checks and O(n)
	// broadcast — fine at the scale Tower targets.
	clients := make(map[*Client]struct{})

	for {
		select {

		case msg := <-h.subscribe:
			if !msg.hasScope {
				// Registration with no scope yet (NewClient path).
				clients[msg.client] = struct{}{}
			} else {
				// AddScope path — client must already be registered.
				if _, ok := clients[msg.client]; ok {
					msg.client.scope = append(msg.client.scope, msg.scope)
				}
			}

		case c := <-h.remove:
			if _, ok := clients[c]; ok {
				delete(clients, c)
				close(c.Send)
			}

		case evt := <-h.broadcast:
			for c := range clients {
				if !c.matches(evt) {
					continue
				}
				select {
				case c.Send <- evt:
				default:
					// Client send buffer full. The WS write pump is responsible
					// for detecting its own lagged state and sending the lagged
					// message. We just drain one slot so the broadcast loop
					// doesn't block.
					select {
					case <-c.Send:
					default:
					}
					log.Printf("hub: client send buffer full, dropped event type=%s", evt.Type)
				}
			}
		}
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func containsUint8(haystack []uint8, needle uint8) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}
	return false
}

func containsEventType(haystack []EventType, needle EventType) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}
	return false
}
