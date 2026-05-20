// Package ingest subscribes to a single MeshCore MQTT broker and drives the
// observation pipeline described in the design doc.
//
// Call Start() once per broker in a dedicated goroutine. Both broker instances
// share the same *hub.Hub and *DB handle so dedup and fan-out are centralised.
//
// Pipeline per incoming /packets message:
//  1. Parse topic → extract IATA + publisher pubkey
//  2. Decode hex payload via meshcore-go PacketFromBytes
//  3. Compute content-based packet hash (PacketHash)
//  4. Upsert observers + observer_brokers + iata_codes
//  5. Upsert packets row (ON CONFLICT bump last_heard_at + observation_count)
//  6. Insert packet_observations (ON CONFLICT DO NOTHING for cross-broker dedup)
//  7. If INSERT succeeded: capability detection, payload-type side effects, fan-out
//
// Pipeline per incoming /status message:
//  1. Parse topic → extract publisher pubkey
//  2. Upsert observers row (status_metadata, last_status_at, observer_type, etc.)
//  3. Fan out observerStatus event to hub
package ingest

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/meshcore-go/meshcore-go"

	"tower/internal/hub"
)

// Config holds the connection parameters for one broker.
type Config struct {
	// BrokerName is a short human-readable label ("mqtt1", "mqtt2") used in
	// log messages and stored in packet_observations.source_broker.
	BrokerName string

	// URL is the full broker WebSocket URL, e.g. "wss://mqtt1.meshcore.ca/mqtt"
	URL string

	Username string
	Password string
}

// DB is the minimal database interface the ingest pipeline depends on.
// Wire in your real *pgxpool.Pool implementation here.
type DB interface {
	// UpsertObserver upserts the observers row keyed on pubkey.
	UpsertObserver(ctx context.Context, pubkey []byte, iata string) error

	// UpsertObserverBroker records that this observer was seen on brokerName.
	UpsertObserverBroker(ctx context.Context, pubkey []byte, brokerName string) error

	// UpsertIATA auto-creates an iata_codes row if it doesn't exist yet.
	UpsertIATA(ctx context.Context, iata string) error

	// UpsertPacket inserts or bumps the packets row. Returns (isNew, error).
	UpsertPacket(ctx context.Context, p UpsertPacketParams) (bool, error)

	// InsertObservation inserts a packet_observations row.
	// Returns (inserted, error); inserted=false means ON CONFLICT DO NOTHING fired.
	InsertObservation(ctx context.Context, o InsertObservationParams) (bool, error)

	// SetNodeCapability flips supports_multibyte_paths or supports_multibyte_traces
	// for a node, never downgrading an existing TRUE.
	SetNodeCapability(ctx context.Context, nodeID string, paths, traces bool) error

	// UpsertNode upserts a nodes row from an advert payload.
	UpsertNode(ctx context.Context, n UpsertNodeParams) error

	// UpsertNodeIATA upserts a node_iatas row.
	UpsertNodeIATA(ctx context.Context, nodeID string, iata string) error

	// InsertChannelMessage stores a decrypted group text message.
	InsertChannelMessage(ctx context.Context, m InsertChannelMessageParams) error

	// UpdateObserverStatus updates the observer row from a /status message.
	UpdateObserverStatus(ctx context.Context, p UpdateObserverStatusParams) error
}

// UpsertPacketParams mirrors the columns written on packets upsert.
type UpsertPacketParams struct {
	PacketHash     []byte
	RouteType      uint8
	PayloadType    uint8
	PayloadVersion uint8
	TransportCodes []byte // nil if not FLOOD/DIRECT
	RawPayload     []byte
	ParsedPayload  json.RawMessage
	OriginPubkey   []byte
	ChannelHash    []byte
}

// InsertObservationParams mirrors the columns written on packet_observations insert.
type InsertObservationParams struct {
	PacketHash        []byte
	ObserverID        string
	IATA              string
	HeardAt           time.Time
	PathLengthByte    uint8
	HashSize          uint8
	HopCount          uint8
	PathBytes         []byte
	RSSI              int16
	SNR               float32
	PropagationTimeMs int32
	RadioFreqMHz      float32
	SpreadFactor      int16
	BandwidthKHz      float32
	CodingRate        int16
	SourceBroker      string
}

// UpsertNodeParams carries the fields extracted from a payload type 0x04 advert.
type UpsertNodeParams struct {
	PublicKey []byte
	Name      string
	NodeType  uint8 // 1=companion, 2=repeater, 3=room server
	Latitude  *float64
	Longitude *float64
}

// InsertChannelMessageParams carries a decrypted group text message.
type InsertChannelMessageParams struct {
	ChannelID    int
	PacketHash   []byte
	SenderName   string
	SenderPubkey []byte
	Content      string
	SentAt       time.Time
}

// UpdateObserverStatusParams carries the fields parsed from a /status message.
type UpdateObserverStatusParams struct {
	PublicKey       []byte
	StatusMetadata  json.RawMessage
	LastStatusAt    time.Time
	BatteryLevel    *int
	UptimeSeconds   *int64
	SoftwareVersion string
	ObserverType    string // only set if we can detect it; never downgrade to unknown
	DisplayName     string // only set if current value is NULL
}

// ChannelKeyStore is a read-only view of the channel keys loaded from config.
// The ingest layer calls Decrypt and never touches key material directly.
type ChannelKeyStore interface {
	// Decrypt attempts to decrypt groupText bytes using the key for channelHash.
	// Returns ("", false) if the key is unknown.
	Decrypt(channelHash []byte, ciphertext []byte) (plaintext string, ok bool)
}

// Worker holds the dependencies for one broker's ingest loop.
type Worker struct {
	cfg  Config
	db   DB
	hub  *hub.Hub
	keys ChannelKeyStore
}

// New creates an ingest Worker. Call Start() to connect and begin processing.
func New(cfg Config, db DB, h *hub.Hub, keys ChannelKeyStore) *Worker {
	return &Worker{cfg: cfg, db: db, hub: h, keys: keys}
}

// Start connects to the broker and blocks until ctx is cancelled. It
// reconnects automatically on transient failures using paho's built-in
// reconnect logic.
//
// Intended usage: go worker.Start(ctx)
func (w *Worker) Start(ctx context.Context) {
	opts := mqtt.NewClientOptions().
		AddBroker(w.cfg.URL).
		SetClientID(fmt.Sprintf("tower-%s", w.cfg.BrokerName)).
		SetUsername(w.cfg.Username).
		SetPassword(w.cfg.Password).
		SetAutoReconnect(true).
		SetMaxReconnectInterval(30 * time.Second).
		SetOnConnectHandler(func(c mqtt.Client) {
			log.Printf("ingest[%s]: connected to %s", w.cfg.BrokerName, w.cfg.URL)
			w.subscribe(c)
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			log.Printf("ingest[%s]: connection lost: %v", w.cfg.BrokerName, err)
		})

	client := mqtt.NewClient(opts)
	if tok := client.Connect(); tok.Wait() && tok.Error() != nil {
		log.Printf("ingest[%s]: initial connect failed: %v", w.cfg.BrokerName, tok.Error())
		// paho will retry; we fall through and wait for ctx
	}

	<-ctx.Done()
	client.Disconnect(500)
	log.Printf("ingest[%s]: stopped", w.cfg.BrokerName)
}

// subscribe registers the wildcard topic handler after (re)connect.
func (w *Worker) subscribe(client mqtt.Client) {
	// meshcore/{IATA}/{pubkey}/packets
	// meshcore/{IATA}/{pubkey}/status
	// We do NOT subscribe to /internal (Role 2 access).
	tok := client.Subscribe("meshcore/#", 1, func(_ mqtt.Client, msg mqtt.Message) {
		w.handleMessage(msg)
	})
	if tok.Wait() && tok.Error() != nil {
		log.Printf("ingest[%s]: subscribe error: %v", w.cfg.BrokerName, tok.Error())
	}
}

// handleMessage dispatches incoming MQTT messages by subtopic.
func (w *Worker) handleMessage(msg mqtt.Message) {
	// Topic shape: meshcore/{IATA}/{pubkey}/{subtopic}
	parts := strings.SplitN(msg.Topic(), "/", 4)
	if len(parts) != 4 || parts[0] != "meshcore" {
		return
	}
	iata, pubkeyHex, subtopic := parts[1], parts[2], parts[3]

	ctx := context.Background()

	switch subtopic {
	case "packets":
		w.handlePacket(ctx, iata, pubkeyHex, msg.Payload())
	case "status":
		w.handleStatus(ctx, pubkeyHex, msg.Payload())
		// "internal" is intentionally not handled (Role 2 access)
	}
}

// handlePacket runs the full observation pipeline for a /packets message.
func (w *Worker) handlePacket(ctx context.Context, iata, pubkeyHex string, raw []byte) {
	var envelope struct {
		Raw        string `json:"raw"` // hex-encoded raw LoRa packet bytes
		Timestamp  string `json:"timestamp"`
		Hash       string `json:"hash"`
		Origin     string `json:"origin"`
		Type       string `json:"type"`
		Direction  string `json:"direction"`
		Time       string `json:"time"`
		Date       string `json:"date"`
		Len        string `json:"len"`
		PacketType string `json:"packet_type"`
		Route      string `json:"route"`
		PayloadLen string `json:"payload_len"`
		OriginID   string `json:"origin_id"`
		SNR        string `json:"SNR"`
		RSSI       string `json:"RSSI"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil || envelope.Raw == "" {
		log.Printf("ingest[%s]: malformed packet envelope from %s/%s", w.cfg.BrokerName, iata, pubkeyHex)
		return
	}
	hexBytes, err := hex.DecodeString(envelope.Raw)
	if err != nil {
		log.Printf("ingest[%s]: invalid hex from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
	}

	// ── Step 2: decode via meshcore-go ──────────────────────────────────────
	packet, err := meshcore.PacketFromBytes(hexBytes)
	if err != nil {
		log.Printf("ingest[%s]: error decoding packet from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
		return
	}
	packetHash := packet.PacketHash()
	pathHashes := packet.PathHashes()
	parsed := packet.PayloadTypeString()
	// NOTE:
	// For now we just log and return so the scaffolding compiles.
	log.Printf("ingest[%s]: packet from %s/%s", w.cfg.BrokerName, iata, pubkeyHex)
	log.Printf("hash[%x]: path[%x], payload type: %s", packetHash, pathHashes, parsed)

	// ── Step 3–6: DB writes ──────────────────────────────────────────────────
	// TODO: fill in once meshcore-go is wired.
	//
	//   _ = w.db.UpsertObserver(ctx, pubkeyBytes, iata)
	//   _ = w.db.UpsertObserverBroker(ctx, pubkeyBytes, w.cfg.BrokerName)
	//   _ = w.db.UpsertIATA(ctx, iata)
	//   isNew, _ := w.db.UpsertPacket(ctx, UpsertPacketParams{...})
	//   inserted, _ := w.db.InsertObservation(ctx, InsertObservationParams{...})

	// ── Step 7: side effects (only if observation INSERT succeeded) ──────────
	// TODO:
	//   if inserted {
	//       w.runCapabilityDetection(ctx, packet, iata)
	//       w.handlePayloadTypeSideEffects(ctx, packet, iata)
	//       w.fanOut(packet, observation, isNew)
	//   }

	_ = ctx // suppress unused warning until TODOs are filled in
}

// handleStatus processes a /status message and fans out an observerStatus event.
func (w *Worker) handleStatus(ctx context.Context, pubkeyHex string, raw []byte) {
	// TODO: parse status JSON, call w.db.UpdateObserverStatus, fan out event.
	//
	//   params := UpdateObserverStatusParams{...}
	//   _ = w.db.UpdateObserverStatus(ctx, params)
	//
	//   payload, _ := json.Marshal(statusEvent{...})
	//   w.hub.Broadcast(hub.Event{Type: hub.EventObserverStatus, Payload: payload})

	log.Printf("ingest[%s]: status from %s (TODO)", w.cfg.BrokerName, pubkeyHex)
	_ = ctx
}

// runCapabilityDetection checks hash sizes and flips firmware capability flags.
// Called only when the observation INSERT succeeded (no dedup conflict).
//
// Rules (from design doc):
//   - hash_size == 1: do nothing (proves nothing about firmware)
//   - duplicate hash prefixes within the path: skip entirely
//   - non-trace + hash_size 2 or 3 → supports_multibyte_paths = TRUE
//   - trace (0x09)  + hash_size 2 or 4 → supports_multibyte_traces = TRUE
//
// TODO: implement once meshcore-go PathHashes() is wired.
func (w *Worker) runCapabilityDetection(ctx context.Context, payloadType uint8, hashSize uint8, resolvedNodeIDs []string) {
	if hashSize < 2 {
		return
	}
	for _, nodeID := range resolvedNodeIDs {
		switch {
		case payloadType != 0x09 && (hashSize == 2 || hashSize == 3):
			_ = w.db.SetNodeCapability(ctx, nodeID, true, false)
		case payloadType == 0x09 && (hashSize == 2 || hashSize == 4):
			_ = w.db.SetNodeCapability(ctx, nodeID, false, true)
		}
	}
}

// fanOut builds and broadcasts the packetObservation event to connected WS clients.
func (w *Worker) fanOut(packetHash string, payloadType uint8, iata string, isFirst bool, observationCount int64, payload json.RawMessage) {
	evt := hub.Event{
		Type:        hub.EventPacketObservation,
		Payload:     payload,
		IATA:        iata,
		PayloadType: payloadType,
	}
	w.hub.Broadcast(evt)
}
