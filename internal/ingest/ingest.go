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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"

	"github.com/MeshCore-Tower/tower-server/internal/hub"
	"github.com/MeshCore-Tower/tower-server/internal/keystore"
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

	// UpsertNode upserts a nodes row from an advert payload.
	UpsertNode(ctx context.Context, n UpsertNodeParams) (uuid.UUID, error)

	// UpsertNodeIATA upserts a node_iatas row.
	UpsertNodeIATA(ctx context.Context, nodeID uuid.UUID, iata string) error

	// InsertChannelMessage stores a decrypted group text message.
	InsertChannelMessage(ctx context.Context, m InsertChannelMessageParams) error

	// UpdateObserverStatus updates the observer row from a /status message. Returns the OberserID
	// and any error.
	UpdateObserverStatus(ctx context.Context, p UpdateObserverStatusParams) (uuid.UUID, error)

	// GetObserverLastIATA returns the IATA from the most recent observation for the given observer.
	GetObserverLastIATA(ctx context.Context, observerID uuid.UUID) (string, error)

	// GetObserverRadio returns the current radio settings for the given observer.
	GetObserverRadio(ctx context.Context, observerID uuid.UUID) (RadioSettings, error)

	// ResolvePathHashes returns a list of node UUIDs for the given path hash prefixes and IATA.
	ResolvePathHashes(ctx context.Context, iata string, hashes [][]byte) ([]uuid.UUID, error)

	// UpsertChannel upserts a channel row by (hash, keyFingerprint) and returns its integer ID.
	// Pass nil keyFingerprint to record a hash-only row when the key is unknown.
	UpsertChannel(ctx context.Context, channelHash []byte, keyFingerprint []byte, name string, hashtag string) (int, error)

	// UpsertChannelHashOnly upserts a hash-only channel row for cases where the
	// channel key is unknown. Uses the partial unique index to ensure only one
	// hash-only row exists per channel hash. The return value is the channel ID
	// but can be safely ignored since unknown-key channels have no messages.
	UpsertChannelHashOnly(ctx context.Context, channelHash []byte) (int, error)
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
	ObserverID        uuid.UUID
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
	ChannelID  int
	PacketHash []byte
	SenderName string
	Content    string
	SentAt     time.Time
}

// UpdateObserverStatusParams carries the fields parsed from a /status message.
type UpdateObserverStatusParams struct {
	PublicKey       []byte
	StatusMetadata  json.RawMessage
	LastStatusAt    time.Time
	BatteryLevel    *float32
	UptimeSeconds   *int64
	SoftwareVersion string
	ObserverType    string // only set if we can detect it; never downgrade to unknown
	DisplayName     string // only set if current value is NULL
	HardwareModel   string
	FirmwareVersion string
	FirmwareBuild   string
	RadioFreqMHz    float32
	RadioSF         int16
	RadioBWKHz      float32
	RadioCR         int16
}

// RadioSettings holds the radio configuration for an observer, populated from
// /status messages and copied onto each observation row for RF analysis.
type RadioSettings struct {
	FreqMHz float32 // MHz, e.g. 910.525
	SF      int16   // LoRa spreading factor, e.g. 7
	BWKHz   float32 // bandwidth in kHz, e.g. 62.5
	CR      int16   // coding rate denominator, e.g. 5 means 4/5
}

// statusEvent is the JSON payload for an observerStatus WS event.
// Shape matches the design doc § Server → Client events.
type statusEvent struct {
	ObserverID    string `json:"observerId"`
	DisplayName   string `json:"displayName"`
	IATA          string `json:"iata,omitempty"`
	Online        bool   `json:"online"`
	BatteryMV     int    `json:"batteryMv,omitempty"`
	UptimeSeconds int64  `json:"uptimeSeconds"`
	LastStatusAt  int64  `json:"lastStatusAt"` // epoch ms
}

// channelMessageEvent is the JSON payload for a channelMessage WS event.
type channelMessageEvent struct {
	ChannelID   int    `json:"channelId"`
	ChannelHash string `json:"channelHash"` // hex-encoded single byte
	PacketHash  string `json:"packetHash"`  // hex-encoded
	SenderName  string `json:"senderName"`
	Content     string `json:"content"`
	SentAt      int64  `json:"sentAt"` // epoch ms
}

// nodeUpdateEvent is the JSON payload for a nodeUpdate WS event.
type nodeUpdateEvent struct {
	NodeID   string   `json:"nodeId"` // UUID string
	Name     string   `json:"name"`
	NodeType uint8    `json:"nodeType"`
	IATA     string   `json:"iata"`
	Lat      *float64 `json:"lat,omitempty"`
	Lng      *float64 `json:"lng,omitempty"`
}

// packetObservationEvent is the JSON payload for a packetObservation WS event.
// Shape matches the design doc § Server → Client events.
type packetObservationEvent struct {
	PacketHash string `json:"packetHash"`
	Packet     struct {
		PayloadType        uint8  `json:"payloadType"`
		PayloadTypeName    string `json:"payloadTypeName"`
		RouteType          uint8  `json:"routeType"`
		IsFirstObservation bool   `json:"isFirstObservation"`
	} `json:"packet"`
	Observation struct {
		ObserverID   string  `json:"observerId"`
		ObserverName string  `json:"observerName"`
		IATA         string  `json:"iata"`
		HeardAt      int64   `json:"heardAt"`
		RSSI         int16   `json:"rssi"`
		SNR          float32 `json:"snr"`
		SourceBroker string  `json:"sourceBroker"`
	} `json:"observation"`
}

// ChannelKeyStore is a read-only view of the channel keys loaded from config.
type ChannelKeyStore interface {
	// GetKey returns all known key entries for the given channel hash byte.
	// Returns nil if no keys are known for this hash.
	GetKey(channelHash []byte) []keystore.Entry
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

// handlePacket runs the full observation pipeline for a /packets message.
func (w *Worker) handlePacket(ctx context.Context, iata, pubkeyHex string, raw []byte) {
	var envelope struct {
		Raw        string          `json:"raw"` // hex-encoded raw LoRa packet bytes
		Timestamp  string          `json:"timestamp"`
		Hash       string          `json:"hash"`
		Origin     string          `json:"origin"`
		Type       string          `json:"type"`
		Direction  string          `json:"direction"`
		Time       string          `json:"time"`
		Date       string          `json:"date"`
		Len        string          `json:"len"`
		PacketType string          `json:"packet_type"`
		Route      string          `json:"route"`
		PayloadLen string          `json:"payload_len"`
		OriginID   string          `json:"origin_id"`
		SNR        json.RawMessage `json:"SNR"`
		RSSI       json.RawMessage `json:"RSSI"`
	}
	err := json.Unmarshal(raw, &envelope)
	if err != nil {
		log.Printf("ingest[%s]: malformed packet envelope from %s/%s", w.cfg.BrokerName, iata, pubkeyHex)
		return
	}
	if envelope.Raw == "" {
		if envelope.Len == "0" || envelope.Len == "" {
			return // observer keepalive with no packet data
		}
		log.Printf("ingest[%s]: malformed packet envelope from %s/%s", w.cfg.BrokerName, iata, pubkeyHex)
		return
	}
	hexBytes, err := hex.DecodeString(strings.ReplaceAll(envelope.Raw, " ", ""))
	if err != nil {
		log.Printf("ingest[%s]: invalid hex from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
		return
	}

	packet, err := meshcore.PacketFromBytes(hexBytes)
	if err != nil {
		log.Printf("ingest[%s]: error decoding packet from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
		return
	}

	pubkeyBytes, err := hex.DecodeString(pubkeyHex)
	if err != nil {
		log.Printf("ingest[%s]: invalid pubkey hex from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
		return
	}

	id, observerName, err := w.db.UpsertObserver(ctx, pubkeyBytes)
	if err != nil {
		log.Printf("ingest[%s]: db: upsert observer failed with packet from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
		return
	}
	err = w.db.UpsertObserverBroker(ctx, id, w.cfg.BrokerName)
	if err != nil {
		log.Printf("ingest[%s]: db: update observer broker failed with packet from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
		return
	}
	err = w.db.UpsertIATA(ctx, iata)
	if err != nil {
		log.Printf("ingest[%s]: db: upsert IATA failed with packet from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
		return
	}
	packetHash := packet.PacketHash()
	var transportCodes []byte
	if packet.IsTransport() {
		transportCodes = make([]byte, 4)
		binary.LittleEndian.PutUint16(transportCodes[0:2], packet.TransportCode1)
		binary.LittleEndian.PutUint16(transportCodes[2:4], packet.TransportCode2)
	}
	parsedPayload, err := json.Marshal(packet.Payload)
	if err != nil {
		log.Printf("ingest[%s]: failed to marshal payload from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
		return
	}
	var channelHash []byte
	if packet.PayloadType() == meshcore.PayloadTypeGrpTxt {
		grpTxt, err := meshcore.GroupTextFromBytes(packet.Payload)
		if err != nil {
			log.Printf("ingest[%s]: error decoding group text payload from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
			return
		}
		channelHash = []byte{grpTxt.ChannelHash}
	}
	pParams := UpsertPacketParams{
		PacketHash:     packetHash[:],
		RouteType:      packet.RouteType(),
		PayloadType:    packet.PayloadType(),
		PayloadVersion: packet.PayloadVer(),
		TransportCodes: transportCodes,
		RawPayload:     hexBytes,
		ParsedPayload:  parsedPayload,
		OriginPubkey:   pubkeyBytes,
		ChannelHash:    channelHash,
	}
	isNew, err := w.db.UpsertPacket(ctx, pParams)
	if err != nil {
		log.Printf("ingest[%s]: db: upsert packet failed from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
		return
	}
	heardAt, err := time.Parse("2006-01-02T15:04:05.000000", envelope.Timestamp)
	if err != nil {
		heardAt, err = time.Parse("2006-01-02T15:04:05", envelope.Timestamp)
		if err != nil {
			log.Printf("ingest[%s]: error parsing time from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
			return
		}
	}

	radio, err := w.db.GetObserverRadio(ctx, id)
	if err != nil {
		log.Printf("ingest[%s]: db: get observer radio failed for %s: %v", w.cfg.BrokerName, pubkeyHex, err)
	}
	oParams := InsertObservationParams{
		PacketHash:        packetHash[:],
		ObserverID:        id,
		IATA:              iata,
		HeardAt:           heardAt,
		PathLengthByte:    packet.PathLength,
		HashSize:          packet.PathHashSize(),
		HopCount:          packet.PathHashCount(),
		PathBytes:         packet.Path,
		RSSI:              int16(parseNumber(envelope.RSSI)),
		SNR:               float32(parseNumber(envelope.SNR)),
		PropagationTimeMs: 0, // TODO: figure out how to calculate this
		RadioFreqMHz:      radio.FreqMHz,
		SpreadFactor:      radio.SF,
		BandwidthKHz:      radio.BWKHz,
		CodingRate:        radio.CR,
		SourceBroker:      w.cfg.BrokerName,
	}
	inserted, err := w.db.InsertObservation(ctx, oParams)
	if err != nil {
		log.Printf("ingest[%s]: db: insert observation failed from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
		return
	}

	resolvedIDs, err := w.db.ResolvePathHashes(ctx, iata, packet.PathHashes())
	if err != nil {
		log.Printf("ingest[%s]: db: resolve path hashes failed: %v", w.cfg.BrokerName, err)
		resolvedIDs = []uuid.UUID{}
	}
	if inserted {
		w.runCapabilityDetection(ctx, packet.PayloadType(), packet.PathHashSize(), resolvedIDs)
		w.handlePayloadTypeSideEffects(ctx, packet, iata, packetHash[:])
		evt := packetObservationEvent{}
		evt.PacketHash = hex.EncodeToString(packetHash[:])
		evt.Packet.PayloadType = packet.PayloadType()
		evt.Packet.PayloadTypeName = packet.PayloadTypeString()
		evt.Packet.RouteType = packet.RouteType()
		evt.Packet.IsFirstObservation = isNew
		evt.Observation.ObserverID = id.String()
		evt.Observation.ObserverName = observerName
		evt.Observation.IATA = iata
		evt.Observation.HeardAt = heardAt.UnixMilli()
		evt.Observation.RSSI = oParams.RSSI
		evt.Observation.SNR = oParams.SNR
		evt.Observation.SourceBroker = w.cfg.BrokerName
		w.broadcast(hub.EventPacketObservation, iata, packet.PayloadType(), "", evt)
	}
}

// handleStatus processes a /status message and fans out an observerStatus event.
func (w *Worker) handleStatus(ctx context.Context, pubkeyHex string, raw []byte) {
	var envelope struct {
		ObserverType    string `json:"source"`
		SoftwareVersion string `json:"client_version"`
		HardwareModel   string `json:"model"`
		FirmwareVersion string `json:"firmware_version"`
		DisplayName     string `json:"origin"`
		RadioString     string `json:"radio"`
		Stats           struct {
			UptimeSeconds int64   `json:"uptime_secs"`
			BatteryMV     int     `json:"battery_mv"`
			NoiseFloor    float32 `json:"noise_floor"`
			QueueLen      int     `json:"queue_len"`
			DebugFlags    int     `json:"debug_flags"`
			TxAirSecs     float64 `json:"tx_air_secs"`
			RxAirSecs     float64 `json:"rx_air_secs"`
			RecvErrors    int     `json:"recv_errors"`
		} `json:"stats"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		log.Printf("ingest[%s]: malformed status envelope from %s: %v", w.cfg.BrokerName, pubkeyHex, err)
		return
	}
	pubkey, err := hex.DecodeString(pubkeyHex)
	if err != nil {
		log.Printf("ingest[%s]: invalid pubkey hex in status from %s: %v", w.cfg.BrokerName, pubkeyHex, err)
		return
	}
	_, _, err = w.db.UpsertObserver(ctx, pubkey)
	if err != nil {
		log.Printf("ingest[%s]: db: upsert observer failed in status from %s: %v", w.cfg.BrokerName, pubkeyHex, err)
		return
	}
	params := UpdateObserverStatusParams{
		PublicKey:      pubkey,
		StatusMetadata: raw,
		LastStatusAt:   time.Now(),
	}
	params.UptimeSeconds = &envelope.Stats.UptimeSeconds
	if envelope.Stats.BatteryMV != 0 {
		batteryLevel := float32(envelope.Stats.BatteryMV) / 1000
		params.BatteryLevel = &batteryLevel
	}
	if envelope.SoftwareVersion != "" {
		params.SoftwareVersion = envelope.SoftwareVersion
	}
	if envelope.ObserverType != "" {
		params.ObserverType = envelope.ObserverType
	}
	if envelope.DisplayName != "" {
		params.DisplayName = envelope.DisplayName
	}
	if envelope.HardwareModel != "" {
		params.HardwareModel = envelope.HardwareModel
	}
	if envelope.FirmwareVersion != "" {
		params.FirmwareVersion = envelope.FirmwareVersion
	}

	radio := strings.Split(strings.TrimSpace(envelope.RadioString), ",")
	if len(radio) != 4 {
		log.Printf("ingest[%s]: missing or malformed radio params in status from %s, skipping radio fields", w.cfg.BrokerName, pubkeyHex)
	} else {
		freq, err := strconv.ParseFloat(radio[0], 32)
		if err != nil {
			log.Printf("ingest[%s]: error parsing radio freq in status from %s: %v", w.cfg.BrokerName, pubkeyHex, err)
		} else {
			params.RadioFreqMHz = float32(freq)
		}
		bw, err := strconv.ParseFloat(radio[1], 32)
		if err != nil {
			log.Printf("ingest[%s]: error parsing radio bw in status from %s: %v", w.cfg.BrokerName, pubkeyHex, err)
		} else {
			params.RadioBWKHz = float32(bw)
		}
		sf, err := strconv.ParseInt(radio[2], 10, 16)
		if err != nil {
			log.Printf("ingest[%s]: error parsing radio sf in status from %s: %v", w.cfg.BrokerName, pubkeyHex, err)
		} else {
			params.RadioSF = int16(sf)
		}
		cr, err := strconv.ParseInt(radio[3], 10, 16)
		if err != nil {
			log.Printf("ingest[%s]: error parsing radio cr in status from %s: %v", w.cfg.BrokerName, pubkeyHex, err)
		} else {
			params.RadioCR = int16(cr)
		}
	}

	observerID, err := w.db.UpdateObserverStatus(ctx, params)
	if err != nil {
		log.Printf("ingest[%s]: db: update observer status failed for %s: %v", w.cfg.BrokerName, pubkeyHex, err)
		return
	}
	iata, err := w.db.GetObserverLastIATA(ctx, observerID)
	if err != nil {
		iata = "" // non-fatal, continue
	}
	evt := statusEvent{
		ObserverID:    observerID.String(),
		DisplayName:   envelope.DisplayName,
		IATA:          iata,
		Online:        true,
		BatteryMV:     envelope.Stats.BatteryMV,
		UptimeSeconds: envelope.Stats.UptimeSeconds,
		LastStatusAt:  time.Now().UnixMilli(),
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		log.Printf("ingest[%s]: failed to marshal status event payload for %s: %v", w.cfg.BrokerName, pubkeyHex, err)
		return
	}
	w.hub.Broadcast(hub.Event{Type: hub.EventObserverStatus, Payload: payload, IATA: iata})
}

// runCapabilityDetection checks hash sizes and flips firmware capability flags.
// Called only when the observation INSERT succeeded (no dedup conflict).
//
// Rules (from design doc):
//   - hash_size == 1: do nothing (proves nothing about firmware)
//   - duplicate hash prefixes within the path: skip entirely
//   - non-trace + hash_size 2 or 3 → supports_multibyte_paths = TRUE
//   - trace (0x09)  + hash_size 2 or 4 → supports_multibyte_traces = TRUE
func (w *Worker) runCapabilityDetection(ctx context.Context, payloadType uint8, hashSize uint8, resolvedNodeIDs []uuid.UUID) {
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

// handlePayloadTypeSideEffects runs payload-type-specific processing after a
// new observation is confirmed inserted. Currently handles:
//   - PayloadTypeAdvert (0x04): upsert node and node_iatas
//   - PayloadTypeGrpTxt (0x05): decrypt and store channel message if key is known
func (w *Worker) handlePayloadTypeSideEffects(ctx context.Context, packet *meshcore.Packet, iata string, packetHash []byte) {
	if packet.PayloadType() == meshcore.PayloadTypeAdvert {
		advert, err := meshcore.AdvertFromBytes(packet.Payload)
		if err != nil {
			log.Printf("ingest[%s]: error decoding advert payload: %v", w.cfg.BrokerName, err)
			return
		}
		var lat, lon *float64
		if advert.AppData().Lat != 0 || advert.AppData().Lon != 0 {
			la := float64(advert.AppData().Lat)
			lo := float64(advert.AppData().Lon)
			lat = &la
			lon = &lo
		}
		params := UpsertNodeParams{
			PublicKey: advert.PublicKey.PublicKeyBytes(),
			Name:      advert.AppData().Name,
			NodeType:  advert.Type(),
			Latitude:  lat,
			Longitude: lon,
		}
		nodeID, err := w.db.UpsertNode(ctx, params)
		if err != nil {
			log.Printf("ingest[%s]: db: upsert node failed: %v", w.cfg.BrokerName, err)
			return
		}
		if err := w.db.UpsertNodeIATA(ctx, nodeID, iata); err != nil {
			log.Printf("ingest[%s]: db: upsert node IATA failed: %v", w.cfg.BrokerName, err)
		}
		evt := nodeUpdateEvent{
			NodeID:   nodeID.String(),
			Name:     advert.AppData().Name,
			NodeType: advert.Type(),
			IATA:     iata,
			Lat:      lat,
			Lng:      lon,
		}
		w.broadcast(hub.EventNodeUpdate, iata, meshcore.PayloadTypeAdvert, "", evt)
		return
	}
	if packet.PayloadType() == meshcore.PayloadTypeGrpTxt {
		grpTxt, err := meshcore.GroupTextFromBytes(packet.Payload)
		if err != nil {
			log.Printf("ingest[%s]: error decoding group text payload: %v", w.cfg.BrokerName, err)
			return
		}
		channelHashBytes := []byte{grpTxt.ChannelHash}

		// Always upsert a hash-only row so unknown channels are recorded.
		_, _ = w.db.UpsertChannelHashOnly(ctx, channelHashBytes)

		// Try each known key entry for this hash.
		entries := w.keys.GetKey(channelHashBytes)
		if len(entries) == 0 {
			return // channel key unknown; message stored as encrypted blob only
		}
		var payload *meshcore.GroupTextPayload
		var usedEntry keystore.Entry
		for _, entry := range entries {
			if p, err := grpTxt.DecryptStruct(entry.Key); err == nil {
				payload = p
				usedEntry = entry
				break
			}
		}
		if payload == nil {
			return // none of the keys worked
		}

		// Upsert the keyed channel row — messages are associated with this row.
		channelID, err := w.db.UpsertChannel(ctx, channelHashBytes, usedEntry.Fingerprint, usedEntry.Name, usedEntry.Hashtag)
		if err != nil {
			log.Printf("ingest[%s]: db: upsert keyed channel failed: %v", w.cfg.BrokerName, err)
			return
		}
		params := InsertChannelMessageParams{
			ChannelID:  channelID,
			PacketHash: packetHash[:],
			SenderName: payload.Sender,
			SentAt:     time.Unix(int64(payload.Timestamp), 0),
			Content:    payload.Text,
		}
		err = w.db.InsertChannelMessage(ctx, params)
		if err != nil {
			log.Printf("ingest[%s]: db: insert channel message failed: %v", w.cfg.BrokerName, err)
		}

		evt := channelMessageEvent{
			ChannelID:   channelID,
			ChannelHash: hex.EncodeToString(channelHashBytes),
			PacketHash:  hex.EncodeToString(packetHash),
			SenderName:  payload.Sender,
			Content:     payload.Text,
			SentAt:      time.Unix(int64(payload.Timestamp), 0).UnixMilli(),
		}
		w.broadcast(hub.EventChannelMessage, iata, 0, fmt.Sprintf("%02x", grpTxt.ChannelHash), evt)
		return
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
