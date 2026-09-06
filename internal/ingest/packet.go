// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
)

// UpsertPacketParams mirrors the columns written on packets upsert.
type UpsertPacketParams struct {
	PacketHash     []byte
	RouteType      uint8
	PayloadType    uint8
	PayloadVersion uint8
	TransportCodes []byte // nil if not FLOOD/DIRECT
	RawHeader      []byte
	RawPayload     []byte
	ParsedPayload  json.RawMessage
	OriginPubkey   []byte
	ChannelHash    []byte
	ScopeID        *int32
	TraceTag       []byte
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
	PayloadType       int16
	ResolvedEndpoints json.RawMessage
}

// RadioSettings holds the radio configuration for an observer, populated from
// /status messages and copied onto each observation row for RF analysis.
type RadioSettings struct {
	FreqMHz float32 // MHz, e.g. 910.525
	SF      int16   // LoRa spreading factor, e.g. 7
	BWKHz   float32 // bandwidth in kHz, e.g. 62.5
	CR      int16   // coding rate denominator, e.g. 5 means 4/5
}

// packetObservationEvent is the JSON payload for a packetObservation WS event.
// Shape matches the design doc § Server → Client events.
type packetObservationEvent struct {
	PacketHash string `json:"packetHash"`
	Packet     struct {
		PayloadType        uint8   `json:"payloadType"`
		PayloadTypeName    string  `json:"payloadTypeName"`
		RouteType          uint8   `json:"routeType"`
		RouteTypeName      string  `json:"routeTypeName"`
		IsFirstObservation bool    `json:"isFirstObservation"`
		ObservationCount   int64   `json:"observationCount"`
		Scope              *string `json:"scope,omitempty"`
	} `json:"packet"`
	Observation struct {
		ObserverID   string  `json:"observerId"`
		ObserverName string  `json:"observerName"`
		IATA         string  `json:"iata"`
		HeardAt      int64   `json:"heardAt"`
		RSSI         int16   `json:"rssi"`
		SNR          float32 `json:"snr"`
		SourceBroker string  `json:"sourceBroker"`
		PathBytes    string  `json:"pathBytes"`
		PathLength   struct {
			Raw      string `json:"raw"`
			HashSize uint8  `json:"hashSize"`
			HopCount uint8  `json:"hopCount"`
		} `json:"pathLength"`
		PropagationTimeMs int32             `json:"propagationTimeMs"`
		ResolvedPath      []api.ResolvedHop `json:"resolvedPath"` // only present in the resolvePath-opted-in variant; see hub.Event.PayloadResolved
		// ResolvedSource/ResolvedDestination mirror api.PacketObservationDetail's fields of
		// the same name -- nil when this payload type has no resolvable endpoint.
		ResolvedSource      *api.ResolvedHop `json:"resolvedSource,omitempty"`
		ResolvedDestination *api.ResolvedHop `json:"resolvedDestination,omitempty"`
	} `json:"observation"`
}

// parsedAnonReq is the parsed form of an ANON_REQ payload. Per
// coreprotocol.org §2.10: the sender includes its full public key inline
// (not yet a known contact of the destination) so the destination can
// derive a shared secret; EphemeralPubKey here is that inline sender key.
type parsedAnonReq struct {
	Raw             string `json:"raw"`
	Type            string `json:"type"`
	Destination     byte   `json:"destination"`
	EphemeralPubKey string `json:"ephemeralPubKey"` // hex
}

// advertFlags is the decoded form of an advert's app_data flags byte. Per
// coreprotocol.org §2.8.3: DeviceRole occupies the low 4 bits (ADV_TYPE_*),
// and the presence bits occupy the high 4 bits, gating which optional
// fields (location, name, feature1/2) follow in app_data.
type advertFlags struct {
	Raw            string `json:"raw"`
	DeviceRole     int    `json:"deviceRole"`
	DeviceRoleName string `json:"deviceRoleName"`
	HasLocation    bool   `json:"hasLocation"`
	HasName        bool   `json:"hasName"`
	HasFeature1    bool   `json:"hasFeature1"`
	HasFeature2    bool   `json:"hasFeature2"`
}

// advertAppData is the parsed form of an ADVERT payload's app_data section.
// Per coreprotocol.org §2.8.2: fields are present in this fixed order, each
// only if its corresponding flag bit is set; Name, if present, fills the
// remainder of app_data with no null terminator on the wire.
type advertAppData struct {
	Raw       string      `json:"raw"`
	Flags     advertFlags `json:"flags"`
	Latitude  *float64    `json:"latitude"`
	Longitude *float64    `json:"longitude"`
	Feature1  *uint16     `json:"feature1"`
	Feature2  *uint16     `json:"feature2"`
	Name      *string     `json:"name"`
}

// parsedAdvert is the parsed form of an ADVERT payload (§2.8). Unencrypted
// but signed; Signature covers public_key || timestamp || app_data.
type parsedAdvert struct {
	Type      string        `json:"type"`
	Raw       string        `json:"raw"`
	PublicKey string        `json:"publicKey"`
	Timestamp uint32        `json:"timestamp"`
	Signature string        `json:"signature"`
	AppData   advertAppData `json:"appData"`
}

// parsedEnvelope is the parsed form of the shared direct-encrypted wrapper
// used by REQ/RESPONSE/TXT_MSG/PATH payloads (§2.9). Decrypted holds the
// payload-type-specific plaintext once MAC verification and decryption
// succeed, or nil if the observer doesn't hold the relevant key.
type parsedEnvelope struct {
	Raw              string `json:"raw"`
	Type             string `json:"type"`
	DestinationHash  string `json:"destinationHash"`
	SourceHash       string `json:"sourceHash"`
	CipherMac        string `json:"cipherMac"`
	Ciphertext       string `json:"ciphertext"`
	CiphertextLength int    `json:"ciphertextLength"`
	Decrypted        any    `json:"decrypted"`
}

// parsedGroupEnvelope is the parsed form of the shared group-channel
// wrapper used by GRP_TXT/GRP_DATA payloads (§2.11). Decrypted holds the
// payload-type-specific plaintext once MAC verification and decryption
// succeed against a known channel key, or nil otherwise.
type parsedGroupEnvelope struct {
	Raw              string `json:"raw"`
	Type             string `json:"type"`
	ChannelHash      string `json:"channelHash"`
	CipherMac        string `json:"cipherMac"`
	Ciphertext       string `json:"ciphertext"`
	CiphertextLength int    `json:"ciphertextLength"`
	Decrypted        any    `json:"decrypted"`
}

// parsedTrace is the parsed form of a TRACE payload (§2.13). PathHashes is
// the original hop-hash sequence carried in the payload trailer; SNRValues
// is the accumulated per-hop SNR sequence carried in the packet header's
// path field (a protocol-level repurposing distinct from the payload).
type parsedTrace struct {
	Raw        string    `json:"raw"`
	Type       string    `json:"type"`
	TraceTag   string    `json:"traceTag"`
	AuthCode   uint32    `json:"authCode"`
	Flags      byte      `json:"flags"`
	PathHashes []string  `json:"pathHashes"`
	SNRValues  []float32 `json:"snrValues"`
}

// parsedAck is the parsed form of an ACK payload (§2.12). Checksum is the
// first 4 bytes of a SHA-256 digest (not a CRC, despite the firmware's
// naming) used as a short identifier of the specific message being
// acknowledged.
type parsedAck struct {
	Raw      string `json:"raw"`
	Type     string `json:"type"`
	Checksum string `json:"checksum"`
}

// parsedMultipart is the parsed form of a MULTIPART payload (§2.14),
// currently used only for multi-ACK bursts. Remaining is the number of
// additional bursts expected; WrappedType/WrappedPayload are the
// encapsulated sub-payload (e.g. an ACK).
type parsedMultipart struct {
	Raw            string `json:"raw"`
	Type           string `json:"type"`
	Remaining      uint8  `json:"remaining"`
	WrappedType    byte   `json:"wrappedType"`
	WrappedPayload string `json:"wrappedPayload"`
}

// parsedDiscoverReq is the parsed form of a CONTROL/DISCOVER_REQ payload.
// Per coreprotocol.org §2.15.1: type_filter is a bitfield where bit n means
// "wants responses from ADV_TYPE_n", and since is an optional last-advert
// cutoff. Repeaters rarely originate these, so the contained SNR-adjacent
// data is mostly useful for UI display rather than analysis.
type parsedDiscoverReq struct {
	Raw        string `json:"raw"`
	Type       string `json:"type"`
	PrefixOnly bool   `json:"prefixOnly"`
	TypeFilter byte   `json:"typeFilter"`
	Tag        string `json:"tag"` // hex-encoded uint32, matches traceTag/checksum style elsewhere
	Since      *int64 `json:"since,omitempty"`
}

// parsedDiscoverResp is the parsed form of a CONTROL/DISCOVER_RESP payload.
// Per coreprotocol.org §2.15.2: flags low nibble is the responder's
// ADV_TYPE_*, snr is the responder's reading of the *request* packet (a
// node-to-node measurement, not the observer's reception quality — see
// PubKeyPrefixOnly below for why we don't always have a resolvable node).
type parsedDiscoverResp struct {
	Raw              string  `json:"raw"`
	Type             string  `json:"type"`
	NodeType         byte    `json:"nodeType"`
	NodeTypeName     string  `json:"nodeTypeName"`
	RequestSNR       float32 `json:"requestSnr"` // responder's measurement of the request packet; node-to-node, not observer reception
	Tag              string  `json:"tag"`
	PubKey           string  `json:"pubKey"`           // hex; full 32 bytes or 8-byte prefix
	PubKeyPrefixOnly bool    `json:"pubKeyPrefixOnly"` // true when PubKey is only an 8-byte prefix (not enough to resolve a node)
}

// parsedControl is the generic fallback shape for any CONTROL sub-type
// (§2.15) not given its own typed parser above (currently everything
// except DISCOVER_REQ/DISCOVER_RESP).
type parsedControl struct {
	Raw   string `json:"raw"`
	Type  string `json:"type"`
	Flags byte   `json:"flags"`
	Data  string `json:"data"`
}

// parsedRaw is the fallback shape for any payload type with no dedicated
// parser, including PAYLOAD_TYPE_RAW_CUSTOM (§2.16), which is opaque to
// the network layer by design.
type parsedRaw struct {
	Type string `json:"type"`
	Raw  string `json:"raw"`
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

	// begin parse payloads
	var channelHash []byte
	originPubkey := []byte(nil)
	var parsedPayload json.RawMessage
	var traceTag []byte
	// For PayloadTypeTrace, packet.Path holds one SNR byte per hop (not hashes -- see
	// below), so the "physical route" hashes for resolvedPath/known-route purposes come
	// instead from the TRACE payload's own embedded PathHashes (the path being probed).
	var traceRawHashes [][]byte
	// 1-byte source/destination hashes for ambiguous prefix resolution (REQUEST, RESPONSE,
	// TEXT_MESSAGE, PATH, ANON_REQ's destination). GRP_TXT/GRP_DATA/TRACE have no such
	// fields and are left nil.
	var sourceHashByte, destHashByte []byte

	switch packet.PayloadType() {
	case meshcore.PayloadTypeGrpTxt:
		grpTxt, err := meshcore.GroupTextFromBytes(packet.Payload)
		if err == nil {
			channelHash = []byte{grpTxt.ChannelHash}
			pg := parsedGroupEnvelope{
				Raw:              hex.EncodeToString(packet.Payload),
				Type:             "GROUP_TEXT",
				ChannelHash:      hex.EncodeToString([]byte{grpTxt.ChannelHash}),
				CipherMac:        hex.EncodeToString(grpTxt.MAC[:]),
				Ciphertext:       hex.EncodeToString(grpTxt.EncryptedPayload),
				CiphertextLength: len(grpTxt.EncryptedPayload),
			}
			parsedPayload, _ = json.Marshal(pg)
		}

	case meshcore.PayloadTypeGrpData:
		grpData, err := meshcore.GroupDataFromBytes(packet.Payload)
		if err == nil {
			pg := parsedGroupEnvelope{
				Raw:              hex.EncodeToString(packet.Payload),
				Type:             "GROUP_DATA",
				ChannelHash:      hex.EncodeToString([]byte{grpData.ChannelHash}),
				CipherMac:        hex.EncodeToString(grpData.MAC[:]),
				Ciphertext:       hex.EncodeToString(grpData.EncryptedPayload),
				CiphertextLength: len(grpData.EncryptedPayload),
			}
			parsedPayload, _ = json.Marshal(pg)
		}

	case meshcore.PayloadTypeAdvert:
		advert, err := meshcore.AdvertFromBytes(packet.Payload)
		if err == nil && advert.Verify() {
			originPubkey = advert.PublicKey.PublicKeyBytes()
			appData := advert.AppData()
			flags := advert.Flags()

			hasLocation := flags&0x10 != 0
			hasFeature1 := flags&0x20 != 0
			hasFeature2 := flags&0x40 != 0
			hasName := flags&0x80 != 0

			var lat, lon *float64
			if hasLocation {
				la := float64(appData.Lat) / 1e6
				lo := float64(appData.Lon) / 1e6
				lat = &la
				lon = &lo
			}

			var feat1, feat2 *uint16
			if hasFeature1 {
				feat1 = &appData.Feat1
			}
			if hasFeature2 {
				feat2 = &appData.Feat2
			}

			var name *string
			if hasName {
				n := strings.ToValidUTF8(appData.Name, "\uFFFD")
				name = &n
			}

			deviceRole := int(flags & 0x0F)

			pa := parsedAdvert{
				Type:      "ADVERT",
				Raw:       hex.EncodeToString(packet.Payload),
				PublicKey: hex.EncodeToString(advert.PublicKey.PublicKeyBytes()),
				Timestamp: advert.Timestamp,
				Signature: hex.EncodeToString(advert.Signature),
				AppData: advertAppData{
					Raw: hex.EncodeToString(advert.RawAppData),
					Flags: advertFlags{
						Raw:            fmt.Sprintf("%02x", flags),
						DeviceRole:     deviceRole,
						DeviceRoleName: appData.Type,
						HasLocation:    hasLocation,
						HasName:        hasName,
						HasFeature1:    hasFeature1,
						HasFeature2:    hasFeature2,
					},
					Latitude:  lat,
					Longitude: lon,
					Feature1:  feat1,
					Feature2:  feat2,
					Name:      name,
				},
			}
			parsedPayload, _ = json.Marshal(pa)
		}

	case meshcore.PayloadTypeAnonReq:
		anonReq, err := meshcore.AnonReqFromBytes(packet.Payload)
		if err == nil {
			originPubkey = anonReq.EphemeralPubKey[:]
			destHashByte = []byte{anonReq.Destination}
			par := parsedAnonReq{
				Raw:             hex.EncodeToString(packet.Payload),
				Type:            "ANON_REQUEST",
				Destination:     anonReq.Destination,
				EphemeralPubKey: hex.EncodeToString(anonReq.EphemeralPubKey[:]),
			}
			parsedPayload, _ = json.Marshal(par)
		}

	case meshcore.PayloadTypeReq:
		req, err := meshcore.RequestFromBytes(packet.Payload)
		if err == nil {
			sourceHashByte = []byte{req.Source}
			destHashByte = []byte{req.Destination}
			pe := parsedEnvelope{
				Raw:              hex.EncodeToString(packet.Payload),
				Type:             "REQUEST",
				DestinationHash:  hex.EncodeToString([]byte{req.Destination}),
				SourceHash:       hex.EncodeToString([]byte{req.Source}),
				CipherMac:        hex.EncodeToString(req.MAC[:]),
				Ciphertext:       hex.EncodeToString(req.EncryptedPayload),
				CiphertextLength: len(req.EncryptedPayload),
			}
			parsedPayload, _ = json.Marshal(pe)
		}

	case meshcore.PayloadTypeResponse:
		resp, err := meshcore.ResponseFromBytes(packet.Payload)
		if err == nil {
			sourceHashByte = []byte{resp.Source}
			destHashByte = []byte{resp.Destination}
			pe := parsedEnvelope{
				Raw:              hex.EncodeToString(packet.Payload),
				Type:             "RESPONSE",
				DestinationHash:  hex.EncodeToString([]byte{resp.Destination}),
				SourceHash:       hex.EncodeToString([]byte{resp.Source}),
				CipherMac:        hex.EncodeToString(resp.MAC[:]),
				Ciphertext:       hex.EncodeToString(resp.EncryptedPayload),
				CiphertextLength: len(resp.EncryptedPayload),
			}
			parsedPayload, _ = json.Marshal(pe)
		}

	case meshcore.PayloadTypeTxtMsg:
		txt, err := meshcore.TextMessageFromBytes(packet.Payload)
		if err == nil {
			sourceHashByte = []byte{txt.Source}
			destHashByte = []byte{txt.Destination}
			pe := parsedEnvelope{
				Raw:              hex.EncodeToString(packet.Payload),
				Type:             "TEXT_MESSAGE",
				DestinationHash:  hex.EncodeToString([]byte{txt.Destination}),
				SourceHash:       hex.EncodeToString([]byte{txt.Source}),
				CipherMac:        hex.EncodeToString(txt.MAC[:]),
				Ciphertext:       hex.EncodeToString(txt.EncryptedPayload),
				CiphertextLength: len(txt.EncryptedPayload),
			}
			parsedPayload, _ = json.Marshal(pe)
		}

	case meshcore.PayloadTypePath:
		path, err := meshcore.PathFromBytes(packet.Payload)
		if err == nil {
			sourceHashByte = []byte{path.Source}
			destHashByte = []byte{path.Destination}
			pe := parsedEnvelope{
				Raw:              hex.EncodeToString(packet.Payload),
				Type:             "PATH",
				DestinationHash:  hex.EncodeToString([]byte{path.Destination}),
				SourceHash:       hex.EncodeToString([]byte{path.Source}),
				CipherMac:        hex.EncodeToString(path.MAC[:]),
				Ciphertext:       hex.EncodeToString(path.EncryptedPayload),
				CiphertextLength: len(path.EncryptedPayload),
			}
			parsedPayload, _ = json.Marshal(pe)
		}

	case meshcore.PayloadTypeTrace:
		trace, err := meshcore.TraceFromBytes(packet.Payload)
		if err == nil {
			traceTag = uint32ToBytes(trace.Tag)
			hashSize := int(trace.PathHashSize())
			hashes := make([]string, 0)
			rawHashes := make([][]byte, 0)
			for i := 0; i+hashSize <= len(trace.PathHashes); i += hashSize {
				h := trace.PathHashes[i : i+hashSize]
				hashes = append(hashes, hex.EncodeToString(h))
				rawHashes = append(rawHashes, h)
			}
			traceRawHashes = rawHashes
			// SNR values are in packet.Path, one signed int8 per consumed hop
			snrValues := make([]float32, 0, len(packet.Path))
			for _, b := range packet.Path {
				snrValues = append(snrValues, float32(int8(b))/4.0)
			}
			traceType := "TRACE"
			if len(hashes) == 1 {
				traceType = "PING"
			}
			pt := parsedTrace{
				Raw:        hex.EncodeToString(packet.Payload),
				Type:       traceType,
				TraceTag:   hex.EncodeToString(uint32ToBytes(trace.Tag)),
				AuthCode:   trace.AuthCode,
				Flags:      trace.Flags,
				PathHashes: hashes,
				SNRValues:  snrValues,
			}
			parsedPayload, _ = json.Marshal(pt)

			// Each consecutive hop pair becomes a node_neighbors edge:
			// hop[i]'s measured SNR of receiving from hop[i-1]. hop[0]'s
			// SNR (snrValues[0]) has no resolvable "previous" node (the
			// originator isn't in PathHashes) and is skipped. Either side
			// resolving ambiguously (>1 candidate) or not at all (0
			// candidates) skips that specific pair.
			if len(rawHashes) >= 2 {
				resolved, rErr := w.db.ResolvePathHashes(ctx, iata, rawHashes)
				if rErr == nil {
					for i := 1; i < len(rawHashes); i++ {
						if i >= len(snrValues) {
							break
						}
						prevEntries := resolved[hashes[i-1]]
						currEntries := resolved[hashes[i]]
						if len(prevEntries) == 1 && len(currEntries) == 1 {
							snr := snrValues[i]
							if err := w.db.UpsertNodeNeighbor(ctx, currEntries[0].NodeID, prevEntries[0].NodeID, iata, &snr, nil); err != nil {
								log.Printf("ingest[%s]: failed to upsert trace neighbor: %v", w.cfg.BrokerName, err)
							}
						}
					}
				}
			}
		}

	case meshcore.PayloadTypeAck:
		ack, err := meshcore.AckFromBytes(packet.Payload)
		if err == nil {
			pa := parsedAck{
				Raw:      hex.EncodeToString(packet.Payload),
				Type:     "ACK",
				Checksum: hex.EncodeToString(uint32ToBytes(ack.CRC())),
			}
			parsedPayload, _ = json.Marshal(pa)
		}

	case meshcore.PayloadTypeMultiPart:
		mp, err := meshcore.MultiPartFromBytes(packet.Payload)
		if err == nil {
			pm := parsedMultipart{
				Raw:            hex.EncodeToString(packet.Payload),
				Type:           "MULTIPART",
				Remaining:      mp.Remaining,
				WrappedType:    mp.WrappedType,
				WrappedPayload: hex.EncodeToString(mp.WrappedPayload),
			}
			parsedPayload, _ = json.Marshal(pm)
		}

	case meshcore.PayloadTypeControl:
		ctrl, err := meshcore.ControlFromBytes(packet.Payload)
		if err == nil {
			switch ctrl.SubType() {
			case meshcore.ControlSubTypeDiscoverReq:
				req, err := ctrl.DiscoverRequest()
				if err == nil {
					pd := parsedDiscoverReq{
						Raw:        hex.EncodeToString(packet.Payload),
						Type:       "DISCOVER_REQ",
						PrefixOnly: req.PrefixOnly,
						TypeFilter: req.TypeFilter,
						Tag:        hex.EncodeToString(uint32ToBytes(req.Tag)),
					}
					if req.Since != 0 {
						since := int64(req.Since)
						pd.Since = &since
					}
					parsedPayload, _ = json.Marshal(pd)
				}
			case meshcore.ControlSubTypeDiscoverResp:
				resp, err := ctrl.DiscoverResponse()
				if err == nil {
					pd := parsedDiscoverResp{
						Raw:              hex.EncodeToString(packet.Payload),
						Type:             "DISCOVER_RESP",
						NodeType:         resp.NodeType,
						NodeTypeName:     api.NodeTypeName(int16(resp.NodeType)),
						RequestSNR:       resp.SNR,
						Tag:              hex.EncodeToString(uint32ToBytes(resp.Tag)),
						PubKey:           hex.EncodeToString(resp.PubKey),
						PubKeyPrefixOnly: len(resp.PubKey) < 32,
					}
					parsedPayload, _ = json.Marshal(pd)

					// Record the observer's own RX SNR of hearing this
					// DISCOVER_RESP as a node_neighbors edge: observer's
					// node -> responder's node. Skipped (not an error) if
					// either the observer or the responder has no node row
					// yet (e.g. neither has advertised), or if the pubkey
					// is only an 8-byte prefix (not enough to resolve).
					if len(resp.PubKey) == 32 {
						observerNodeID, oErr := w.db.GetNodeByPubkey(ctx, pubkeyBytes)
						responderNodeID, rErr := w.db.GetNodeByPubkey(ctx, resp.PubKey)
						// don't insert our own node as a neighbor
						if oErr == nil && rErr == nil && observerNodeID != responderNodeID {
							rxSNR := float32(parseNumber(envelope.SNR))
							if err := w.db.UpsertNodeNeighbor(ctx, observerNodeID, responderNodeID, iata, &rxSNR, nil); err != nil {
								log.Printf("ingest[%s]: failed to upsert observer-discover neighbor: %v", w.cfg.BrokerName, err)
							}
						}
					}
				}
			default:
				pc := parsedControl{
					Raw:   hex.EncodeToString(packet.Payload),
					Type:  "CONTROL",
					Flags: ctrl.Flags,
					Data:  hex.EncodeToString(ctrl.Data),
				}
				parsedPayload, _ = json.Marshal(pc)
			}
		}

	default:
		pr := parsedRaw{
			Type: "RAW",
			Raw:  hex.EncodeToString(packet.Payload),
		}
		parsedPayload, _ = json.Marshal(pr)
	}

	var matchedScope *string
	if packet.RouteType() == meshcore.RouteTypeTransportFlood || packet.RouteType() == meshcore.RouteTypeTransportDirect {
		for _, entry := range w.scopes.Entries() {
			code := computeTransportCode(entry.TransportKey, packet.PayloadType(), packet.Payload)
			if code == packet.TransportCode1 {
				s := entry.Name
				matchedScope = &s
				break
			}
		}
	}
	var scopeID *int32
	if matchedScope != nil {
		id, err := w.db.GetTransportScopeByName(ctx, *matchedScope)
		if err != nil {
			log.Printf("ingest[%s]: failed to get scope ID for %s: %v", w.cfg.BrokerName, *matchedScope, err)
		} else {
			scopeID = &id
		}
	}
	rawHeader := []byte{packet.Header}
	if transportCodes != nil {
		rawHeader = append(rawHeader, transportCodes...)
	}
	pParams := UpsertPacketParams{
		PacketHash:     packetHash[:],
		RouteType:      packet.RouteType(),
		PayloadType:    packet.PayloadType(),
		PayloadVersion: packet.PayloadVer(),
		TransportCodes: transportCodes,
		RawHeader:      rawHeader,
		RawPayload:     packet.Payload,
		ParsedPayload:  parsedPayload,
		OriginPubkey:   originPubkey,
		ChannelHash:    channelHash,
		ScopeID:        scopeID,
		TraceTag:       traceTag,
	}
	isNew, err := w.db.UpsertPacket(ctx, pParams)
	if err != nil {
		log.Printf("ingest[%s]: db: upsert packet failed from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
		return
	}
	// Try parsing with timezone offset first
	heardAt, err := time.Parse("2006-01-02T15:04:05.000000-07:00", envelope.Timestamp)
	if err != nil {
		heardAt, err = time.Parse("2006-01-02T15:04:05.000000", envelope.Timestamp)
	}
	if err != nil {
		heardAt, err = time.Parse("2006-01-02T15:04:05.000000Z", envelope.Timestamp)
	}
	if err != nil {
		heardAt, err = time.Parse("2006-01-02T15:04:05", envelope.Timestamp)
	}
	if err != nil {
		heardAt, err = time.Parse("2006-01-02T15:04:05Z", envelope.Timestamp)
	}
	if err != nil {
		log.Printf("ingest[%s]: failed to parse timestamp %q: %v", w.cfg.BrokerName, envelope.Timestamp, err)
		heardAt = time.Now().UTC()
	} else {
		// clamp to server time if offset is suspicious (> 30 min drift)
		now := time.Now().UTC()
		diff := heardAt.UTC().Sub(now)
		if diff > 30*time.Minute || diff < -30*time.Minute {
			log.Printf("ingest[%s]: clamping suspicious timestamp %s (diff %v) for pubkey %s", w.cfg.BrokerName, envelope.Timestamp, diff, pubkeyHex[:8])
			heardAt = now
		}
	}

	radio, err := w.db.GetObserverRadio(ctx, id)
	if err != nil {
		log.Printf("ingest[%s]: db: get observer radio failed for %s: %v", w.cfg.BrokerName, pubkeyHex, err)
	}
	// Save the same endpoint resolution used by the live event in the observation INSERT.
	var resolvedSource, resolvedDestination *api.ResolvedHop
	if packet.PayloadType() == meshcore.PayloadTypeAdvert && originPubkey != nil {
		// Exact match: ADVERT carries the sender's real identity pubkey, not a
		// short ambiguous hash prefix like the other resolvable payload types.
		if nodeID, err := w.db.GetNodeByPubkey(ctx, originPubkey); err == nil {
			if nodes, err := w.db.GetNodesByIDs(ctx, []uuid.UUID{nodeID}); err == nil {
				hop := api.ResolveExactNode(nodes[nodeID])
				resolvedSource = &hop
			}
		}
	} else if len(sourceHashByte) == 1 {
		if r, err := w.db.ResolvePathHashes(ctx, iata, [][]byte{sourceHashByte}); err == nil {
			hop := api.BuildResolvedPath([][]byte{sourceHashByte}, r)[0]
			resolvedSource = &hop
		}
	}
	if len(destHashByte) == 1 {
		if r, err := w.db.ResolvePathHashes(ctx, iata, [][]byte{destHashByte}); err == nil {
			hop := api.BuildResolvedPath([][]byte{destHashByte}, r)[0]
			resolvedDestination = &hop
		}
	}
	resolvedEndpoints, err := json.Marshal(api.PacketEndpointSnapshot{Source: resolvedSource, Destination: resolvedDestination})
	if err != nil {
		log.Printf("ingest[%s]: endpoint snapshot encoding failed: %v", w.cfg.BrokerName, err)
		resolvedEndpoints = nil // optional enrichment must not discard the observation
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
		PayloadType:       int16(packet.PayloadType()),
		ResolvedEndpoints: resolvedEndpoints,
	}
	inserted, err := w.db.InsertObservation(ctx, oParams)
	if err != nil {
		log.Printf("ingest[%s]: db: insert observation failed from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
		return
	}

	if scopeID != nil && inserted {
		if err := w.db.UpsertObserverScope(ctx, id, *scopeID); err != nil {
			log.Printf("ingest[%s]: failed to upsert observer scope for %s: %v", w.cfg.BrokerName, id, err)
		}
	}

	// Runs on duplicate observations too; the upsert only writes when the row is >1h stale.
	if channelHash != nil {
		if err := w.db.UpsertChannelIATA(ctx, channelHash, iata, heardAt); err != nil {
			log.Printf("ingest[%s]: db: upsert channel IATA failed from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
		}
	}

	// Runs on duplicate observations too; the upsert only writes when the row is >1h stale.
	if traceTag != nil {
		if err := w.db.UpsertTraceIATA(ctx, traceTag, iata, heardAt); err != nil {
			log.Printf("ingest[%s]: db: upsert trace IATA failed from %s/%s: %v", w.cfg.BrokerName, iata, pubkeyHex, err)
		}
	}

	// packet.PathHashes() reads packet.Path as hash-sized chunks, which is only true for
	// ordinary flood/direct-routed packets. TRACE repurposes packet.Path to carry one SNR
	// byte per hop instead, so for TRACE we resolve against the trace payload's own
	// PathHashes (the path being probed) rather than treating SNR bytes as hashes.
	hashes := packet.PathHashes()
	if packet.PayloadType() == meshcore.PayloadTypeTrace {
		hashes = traceRawHashes
	}
	resolved, err := w.db.ResolvePathHashes(ctx, iata, hashes)
	if err != nil {
		log.Printf("ingest[%s]: path resolution failed: %v", w.cfg.BrokerName, err)
	}
	var resolvedIDs []uuid.UUID
	for _, entries := range resolved {
		for _, e := range entries {
			resolvedIDs = append(resolvedIDs, e.NodeID)
		}
	}
	if len(hashes) > 0 && resolved != nil {
		allHigh := true
		nodeIDs := make([]uuid.UUID, 0, len(hashes))
		hashPrefixes := make([][]byte, 0, len(hashes))
		for _, hash := range hashes {
			key := hex.EncodeToString(hash)
			entries := resolved[key]
			if len(entries) != 1 {
				allHigh = false
				break
			}
			nodeIDs = append(nodeIDs, entries[0].NodeID)
			hashPrefixes = append(hashPrefixes, hash)
		}
		if allHigh && len(nodeIDs) > 1 {
			if err := w.db.UpsertKnownRoute(ctx, nodeIDs, hashPrefixes, iata, int32(len(nodeIDs))); err != nil {
				log.Printf("ingest[%s]: failed to upsert known route: %v", w.cfg.BrokerName, err)
			}
		}
	}
	w.runCapabilityDetection(ctx, packet.PayloadType(), packet.PathHashSize(), resolvedIDs)

	if inserted {
		w.handlePayloadTypeSideEffects(ctx, packet, iata, packetHash[:], radio, scopeID, matchedScope, pubkeyBytes, float32(parseNumber(envelope.SNR)))
		evt := packetObservationEvent{}
		evt.PacketHash = hex.EncodeToString(packetHash[:])
		evt.Packet.PayloadType = packet.PayloadType()
		evt.Packet.PayloadTypeName = packet.PayloadTypeString()
		evt.Packet.RouteType = packet.RouteType()
		evt.Packet.RouteTypeName = api.RouteTypeName(int16(packet.RouteType()))
		evt.Packet.IsFirstObservation = isNew
		evt.Observation.ObserverID = id.String()
		evt.Observation.ObserverName = observerName
		evt.Observation.IATA = iata
		evt.Observation.HeardAt = heardAt.UnixMilli()
		evt.Observation.RSSI = oParams.RSSI
		evt.Observation.SNR = oParams.SNR
		evt.Observation.SourceBroker = w.cfg.BrokerName
		evt.Observation.PathBytes = hex.EncodeToString(packet.Path)
		evt.Observation.PathLength.Raw = fmt.Sprintf("%02x", packet.PathLength)
		evt.Observation.PathLength.HashSize = packet.PathHashSize()
		evt.Observation.PathLength.HopCount = packet.PathHashCount()
		evt.Observation.PropagationTimeMs = 0 // not yet calculated
		count, err := w.db.GetPacketObservationCount(ctx, packetHash[:])
		if err != nil {
			log.Printf("ingest[%s]: failed to get observation count: %v", w.cfg.BrokerName, err)
			count = 0
		}
		evt.Packet.ObservationCount = count
		if matchedScope != nil {
			evt.Packet.Scope = matchedScope
		}
		resolvedPath := api.BuildResolvedPath(hashes, resolved)
		evt.Observation.ResolvedSource = resolvedSource
		evt.Observation.ResolvedDestination = resolvedDestination
		w.broadcastPacketObservation(iata, packet.PayloadType(), evt, resolvedPath)
	}
}

// computeTransportCode derives transport_code_1 from a transport key and packet payload.
// code = HMAC-SHA256(key, payload_type_byte || payload)[0:2] as little-endian uint16.
// Coerces reserved values 0x0000 → 0x0001 and 0xFFFF → 0xFFFE per §2.4.
func computeTransportCode(key []byte, payloadType uint8, payload []byte) uint16 {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte{payloadType})
	mac.Write(payload)
	sum := mac.Sum(nil)
	code := uint16(sum[0]) | uint16(sum[1])<<8
	if code == 0x0000 {
		code = 0x0001
	}
	if code == 0xFFFF {
		code = 0xFFFE
	}
	return code
}
