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
//  5. Upsert packets row (ON CONFLICT bump last_heard_at)
//  6. Insert packet_observations (ON CONFLICT DO NOTHING for cross-broker dedup)
//  7. Match transport codes against known scopes (TRANSPORT_FLOOD/DIRECT only)
//  8. If INSERT succeeded: capability detection, payload-type side effects, fan-out
//
// Pipeline per incoming /status message:
//  1. Parse topic → extract publisher pubkey
//  2. Upsert observers row (status_metadata, last_status_at, observer_type, etc.)
//  3. Fan out observerStatus event to hub
package ingest

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"

	"github.com/MeshCore-Tower/tower-server/internal/api"
	"github.com/MeshCore-Tower/tower-server/internal/hub"
	"github.com/MeshCore-Tower/tower-server/internal/keystore"
	"github.com/MeshCore-Tower/tower-server/internal/scopestore"
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

	// TelemetryResolution controls how frequently telemetry snapshots are stored.
	// Status messages within the same window are deduplicated via ON CONFLICT.
	// Defaults to 1 hour if zero.
	TelemetryResolution time.Duration
}

// DB is the minimal database interface the ingest pipeline depends on.
// Wire in your real *pgxpool.Pool implementation here.
type DB interface {
	// UpsertObserver upserts the observers row keyed on pubkey, and returns
	// the Observer ID, Display Name and an error if any.
	UpsertObserver(ctx context.Context, pubkey []byte) (uuid.UUID, string, error)

	// UpsertObserverBroker records that this observer was seen on brokerName.
	UpsertObserverBroker(ctx context.Context, observerID uuid.UUID, brokerName string) error

	// UpsertIATA auto-creates an iata_codes row if it doesn't exist yet.
	UpsertIATA(ctx context.Context, iata string) error

	// UpsertPacket inserts or bumps the packets row. Returns (isNew, error).
	UpsertPacket(ctx context.Context, p UpsertPacketParams) (bool, error)

	// InsertObservation inserts a packet_observations row.
	// Returns (inserted, error); inserted=false means ON CONFLICT DO NOTHING fired.
	InsertObservation(ctx context.Context, o InsertObservationParams) (bool, error)

	// SetNodeCapability flips supports_multibyte_paths or supports_multibyte_traces
	// for a node, never downgrading an existing TRUE.
	SetNodeCapability(ctx context.Context, nodeID uuid.UUID, paths, traces bool) error

	// SetNodeDefaultScope records the most recent scope attached to a node advert
	// scopes are matched against configured regional transport scopes
	SetNodeDefaultScope(ctx context.Context, nodeID uuid.UUID, scopeID int32) error

	// UpsertNode upserts a nodes row from an advert payload.
	UpsertNode(ctx context.Context, n UpsertNodeParams, r RadioSettings) (uuid.UUID, error)

	// UpsertNodeIATA upserts a node_iatas row.
	UpsertNodeIATA(ctx context.Context, nodeID uuid.UUID, iata string) error

	// UpsertNodeShortID upserts a node_short_ids row for path resolution.
	UpsertNodeShortID(ctx context.Context, nodeID uuid.UUID, iata string, prefix4 []byte) error

	// InsertChannelMessage stores a decrypted group text message. Returns insert success and an error.
	InsertChannelMessage(ctx context.Context, m InsertChannelMessageParams) (bool, error)

	// UpdateObserverStatus updates the observer row from a /status message. Returns the OberserID
	// and any error.
	UpdateObserverStatus(ctx context.Context, p UpdateObserverStatusParams) (uuid.UUID, error)

	// GetObserverLastIATA returns the IATA from the most recent observation for the given observer.
	GetObserverLastIATA(ctx context.Context, observerID uuid.UUID) (string, error)

	// InsertObserverTelemetry stores a telemetry snapshot for an observer.
	// The caller should truncate reportedAt to the configured resolution before calling.
	InsertObserverTelemetry(ctx context.Context, observerID uuid.UUID, reportedAt time.Time, batteryMV *int32, txAirSecs, rxAirSecs *float32, noiseFloor float32, uptimeSeconds int64, queueLen, debugFlags, recvErrors *int32) error

	// GetObserverRadio returns the current radio settings for the given observer.
	GetObserverRadio(ctx context.Context, observerID uuid.UUID) (RadioSettings, error)

	// ResolvePathHashes returns a list of node UUIDs for the given path hash prefixes and IATA.
	ResolvePathHashes(ctx context.Context, iata string, hashes [][]byte) (map[string][]api.ResolvedPathEntry, error)

	// UpsertChannel upserts a channel row by (hash, keyFingerprint) and returns its integer ID.
	// Pass nil keyFingerprint to record a hash-only row when the key is unknown.
	UpsertChannel(ctx context.Context, channelHash []byte, keyFingerprint []byte, name string, hashtag string) (int, error)

	// UpsertChannelHashOnly upserts a hash-only channel row for cases where the
	// channel key is unknown. Uses the partial unique index to ensure only one
	// hash-only row exists per channel hash. The return value is the channel ID
	// but can be safely ignored since unknown-key channels have no messages.
	UpsertChannelHashOnly(ctx context.Context, channelHash []byte) (int, error)

	// GetPacketObservationCount returns the number of rows for the packet observations
	GetPacketObservationCount(ctx context.Context, packetHash []byte) (int64, error)

	// GetTransportScopeByName returns the ID of a transport scope by its normalized name.
	GetTransportScopeByName(ctx context.Context, name string) (int32, error)

	// UpsertObserverScope records or updates a scope association for an observer.
	// Called when a TRANSPORT_FLOOD packet is observed, linking the observer to
	// the matched regional transport scope.
	UpsertObserverScope(ctx context.Context, observerID uuid.UUID, scopeID int32) error
}

// ChannelKeyStore is a read-only view of the channel keys loaded from config.
type ChannelKeyStore interface {
	// GetKey returns all known key entries for the given channel hash byte.
	// Returns nil if no keys are known for this hash.
	GetKey(channelHash []byte) []keystore.Entry
}

// ScopeStore provides transport scope key lookup for matching TRANSPORT_FLOOD packets.
type ScopeStore interface {
	Entries() []scopestore.Entry
}

// Worker holds the dependencies for one broker's ingest loop.
type Worker struct {
	cfg    Config
	db     DB
	hub    *hub.Hub
	keys   ChannelKeyStore
	scopes ScopeStore
	client mqtt.Client
}

// New creates an ingest Worker. Call Start() to connect and begin processing.
func New(cfg Config, db DB, h *hub.Hub, keys ChannelKeyStore, scopes ScopeStore) *Worker {
	return &Worker{cfg: cfg, db: db, hub: h, keys: keys, scopes: scopes}
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
		SetKeepAlive(30 * time.Second).
		SetPingTimeout(10 * time.Second).
		SetConnectTimeout(15 * time.Second).
		SetWriteTimeout(10 * time.Second).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetOnConnectHandler(func(c mqtt.Client) {
			log.Printf("ingest[%s]: connected to %s", w.cfg.BrokerName, w.cfg.URL)
			w.subscribe(c)
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			log.Printf("ingest[%s]: connection lost, will reconnect: %v", w.cfg.BrokerName, err)
		})

	w.client = mqtt.NewClient(opts)
	if tok := w.client.Connect(); tok.Wait() && tok.Error() != nil {
		log.Printf("ingest[%s]: initial connect failed: %v", w.cfg.BrokerName, tok.Error())
		// paho will retry; we fall through and wait for ctx
	}

	<-ctx.Done()
	w.client.Disconnect(500)
	log.Printf("ingest[%s]: stopped", w.cfg.BrokerName)
}

func (w *Worker) BrokerName() string {
	return w.cfg.BrokerName
}

func (w *Worker) IsConnected() bool {
	if w.client == nil {
		return false
	}
	return w.client.IsConnected()
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
// Each message is processed with a 30s timeout to prevent slow DB calls
// from blocking the MQTT receive goroutine indefinitely.
func (w *Worker) handleMessage(msg mqtt.Message) {
	// Topic shape: meshcore/{IATA}/{pubkey}/{subtopic}
	parts := strings.SplitN(msg.Topic(), "/", 4)
	if len(parts) != 4 || parts[0] != "meshcore" {
		return
	}
	iata, pubkeyHex, subtopic := parts[1], parts[2], parts[3]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch subtopic {
	case "packets":
		w.handlePacket(ctx, iata, pubkeyHex, msg.Payload())
	case "status":
		w.handleStatus(ctx, pubkeyHex, msg.Payload())
		// "internal" is intentionally not handled (Role 2 access)
	}
}

func (w *Worker) broadcast(eventType hub.EventType, iata string, payloadType uint8, channelHash string, payload any) {
	b, err := json.Marshal(payload)
	if err != nil {
		log.Printf("ingest[%s]: failed to marshal %s event: %v", w.cfg.BrokerName, eventType, err)
		return
	}
	w.hub.Broadcast(hub.Event{
		Type:        eventType,
		Payload:     b,
		IATA:        iata,
		PayloadType: payloadType,
		ChannelHash: channelHash,
	})
}

// parseNumber handles RSSI and SNR fields that different observer types send as
// either a bare JSON number (e.g. -108) or a quoted string (e.g. "-108").
// Returns 0 if the value is missing or unparseable.
func parseNumber(raw json.RawMessage) float64 {
	if len(raw) == 0 {
		return 0
	}
	// try unquoted number first
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return f
	}
	// try quoted string
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		f, _ = strconv.ParseFloat(s, 64)
		return f
	}
	return 0
}

func inferObserverType(source, clientVersion string) string {
	if source != "" {
		return normalizeObserverType(source)
	}
	if clientVersion != "" {
		return clientVersion // use raw version string for custom firmware observers
	}
	return ""
}

func normalizeObserverType(source string) string {
	if source == "" {
		return ""
	}
	s := source
	// strip org prefix e.g. "meshcore-dev/meshcore-ha" → "meshcore-ha"
	if i := strings.LastIndex(s, "/"); i >= 0 {
		s = s[i+1:]
	}
	// strip version suffix e.g. "meshcoretomqtt:1.1.0" → "meshcoretomqtt"
	if i := strings.Index(s, ":"); i >= 0 {
		s = s[:i]
	}
	// strip build suffix e.g. "meshcoretomqtt/1.1.0.0-622ce04" → "meshcoretomqtt"
	if i := strings.Index(s, "/"); i >= 0 {
		s = s[:i]
	}
	if s == "" {
		return source // fall back to raw if parsing produced nothing
	}
	return s
}

func uint32ToBytes(v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return b
}
