// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

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
	"slices"
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
	IATAs         []string
	PayloadTypes  []uint8
	ChannelHashes []string
	Events        []EventType
}

type LaggedNotification struct {
	DroppedCount int
}

// Client represents a connected WebSocket consumer.
type Client struct {
	Send          chan Event
	laggedCH      chan LaggedNotification
	subscriptions map[string]Scope // OR semantics: event matches if it matches any scope entry
}

// matches returns true if the event satisfies at least one of the client's
// active subscriptions.
func (c *Client) matches(e Event) bool {
	if len(c.subscriptions) == 0 {
		return false
	}
	for _, s := range c.subscriptions {
		if scopeMatches(s, e) {
			return true
		}
	}
	return false
}

// LaggedCH returns the channel on which lagged notifications are delivered.
// The WS write pump selects on this alongside Send.
func (c *Client) LaggedCH() <-chan LaggedNotification {
	return c.laggedCH
}

func scopeMatches(s Scope, e Event) bool {
	if len(s.Events) > 0 && !slices.Contains(s.Events, e.Type) {
		return false
	}
	if len(s.IATAs) > 0 && !slices.Contains(s.IATAs, e.IATA) {
		return false
	}
	if len(s.PayloadTypes) > 0 && !slices.Contains(s.PayloadTypes, e.PayloadType) {
		return false
	}
	if len(s.ChannelHashes) > 0 && !slices.Contains(s.ChannelHashes, e.ChannelHash) {
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
	client         *Client
	scope          Scope
	subscriptionID string
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
		Send:          make(chan Event, 256),
		laggedCH:      make(chan LaggedNotification, 8),
		subscriptions: make(map[string]Scope),
	}
	// We don't add it to the map here; we send it through the channel so
	// Run() is the only goroutine that touches the client map.
	h.subscribe <- subscribeMsg{client: c}
	return c
}

// AddScope appends a subscription scope to a client. Called by the WS handler
// when it receives a "subscribe" message from the client.
func (h *Hub) AddScope(c *Client, id string, s Scope) {
	h.subscribe <- subscribeMsg{client: c, scope: s, subscriptionID: id}
}

// RemoveScope removes a single subscription by ID. Called by the WS handler
// when it receives an "unsubscribe" message from the client. Silently ignored
// if the ID is not found.
func (h *Hub) RemoveScope(c *Client, id string) {
	h.unsubscribe <- unsubscribeMsg{client: c, subscriptionID: id}
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
	// broadcast — fine at the scale Beacon targets.
	clients := make(map[*Client]struct{})

	for {
		select {

		case msg := <-h.subscribe:
			if msg.subscriptionID == "" {
				// Registration with no scope yet (NewClient path).
				clients[msg.client] = struct{}{}
			} else {
				// AddScope path — client must already be registered.
				if _, ok := clients[msg.client]; ok {
					msg.client.subscriptions[msg.subscriptionID] = msg.scope
				}
			}

		case msg := <-h.unsubscribe:
			if _, ok := clients[msg.client]; ok {
				delete(msg.client.subscriptions, msg.subscriptionID)
			}

		case c := <-h.remove:
			if _, ok := clients[c]; ok {
				delete(clients, c)
				close(c.Send)
				close(c.laggedCH)
			}

		case evt := <-h.broadcast:
			for c := range clients {
				if !c.matches(evt) {
					continue
				}
				select {
				case c.Send <- evt:
				default:
					dropped := 1
					select {
					case <-c.Send:
					default:
					}
					select {
					case c.laggedCH <- LaggedNotification{DroppedCount: dropped}:
					default:
						// laggedCh itself full; write pump will catch up on next drain
					}
					log.Printf("hub: client send buffer full, dropped event type=%s", evt.Type)
				}
			}
		}
	}
}
