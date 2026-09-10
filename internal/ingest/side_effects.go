// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/meshcore-go/meshcore-go"
)

// UpsertNodeParams carries the fields extracted from a payload type 0x04 advert.
type UpsertNodeParams struct {
	PublicKey []byte
	Name      string
	NodeType  uint8 // 1=companion, 2=repeater, 3=room server
	Latitude  *float64
	Longitude *float64
	// AdvertTimestamp is the device's self-reported wall-clock time (epoch seconds) from the
	// signed advert body. Used to derive clock drift for repeaters/room servers; see
	// api.Node.ClockDriftSeconds.
	AdvertTimestamp uint32
}

// InsertChannelMessageParams carries a decrypted group text message.
type InsertChannelMessageParams struct {
	ChannelID  int
	PacketHash []byte
	SenderName string
	Content    string
	SentAt     time.Time
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
	NodeID       string         `json:"nodeId"`
	PublicKey    string         `json:"publicKey"`
	Name         string         `json:"name"`
	NodeType     uint8          `json:"nodeType"`
	NodeTypeName string         `json:"nodeTypeName"`
	IATA         string         `json:"iata"`
	Lat          *float64       `json:"lat,omitempty"`
	Lng          *float64       `json:"lng,omitempty"`
	IsObserver   bool           `json:"isObserver"`
	IATAs        []api.NodeIATA `json:"iatas"`
	DefaultScope *string        `json:"defaultScope,omitempty"`
	Radio        *string        `json:"radio,omitempty"`
}

// handlePayloadTypeSideEffects runs payload-type-specific processing after a
// new observation is confirmed inserted. Currently handles:
//   - PayloadTypeAdvert (0x04): upsert node and node_iatas
//   - PayloadTypeGrpTxt (0x05): decrypt and store channel message if key is known
func (w *Worker) handlePayloadTypeSideEffects(ctx context.Context, packet *meshcore.Packet, iata string, packetHash []byte, radio RadioSettings, scopeID *int32, matchedScope *string, observerPubkey []byte, rxSNR float32) {
	if packet.PayloadType() == meshcore.PayloadTypeAdvert {
		advert, err := meshcore.AdvertFromBytes(packet.Payload)
		if err != nil {
			log.Printf("ingest[%s]: error decoding advert payload: %v", w.cfg.BrokerName, err)
			return
		}
		if !advert.Verify() {
			log.Printf("ingest[%s]: dropped advert with invalid signature from pubkey %s", w.cfg.BrokerName, hex.EncodeToString(advert.PublicKey.PublicKeyBytes()))
			return
		}
		var lat, lon *float64
		// Presence, not value: an explicit 0/0 advert resets the stored location.
		if advert.Flags()&meshcore.AdvertLatLonMask != 0 {
			la := float64(advert.AppData().Lat) / 1e6
			lo := float64(advert.AppData().Lon) / 1e6
			lat = &la
			lon = &lo
		}
		params := UpsertNodeParams{
			PublicKey:       advert.PublicKey.PublicKeyBytes(),
			Name:            strings.ToValidUTF8(advert.AppData().Name, "\uFFFD"),
			NodeType:        advert.Type(),
			Latitude:        lat,
			Longitude:       lon,
			AdvertTimestamp: advert.Timestamp,
		}
		var nodeRadio RadioSettings
		if packet.PathHashCount() == 0 {
			nodeRadio = radio
		}
		nodeID, err := w.db.UpsertNode(ctx, params, nodeRadio)
		if err != nil {
			log.Printf("ingest[%s]: db: upsert node failed: %v", w.cfg.BrokerName, err)
			return
		}
		// invalidate cache for this node
		if w.onNodeUpsert != nil {
			w.onNodeUpsert(ctx, nodeID)
		}
		// record advertiser neighbors
		// if the advert was forwarded, the first hop is a neighbor
		if packet.PathHashCount() > 0 && (advert.Type() == meshcore.AdvertTypeRepeater || advert.Type() == meshcore.AdvertTypeRoom) {
			firstHop := packet.PathHashes()
			if len(firstHop) > 0 {
				resolved, err := w.db.ResolvePathHashes(ctx, iata, firstHop[:1])
				if err == nil {
					key := hex.EncodeToString(firstHop[0])
					if entries := resolved[key]; len(entries) == 1 {
						if err := w.db.UpsertNodeNeighbor(ctx, nodeID, entries[0].NodeID, iata, nil, nil); err != nil {
							log.Printf("ingest[%s]: failed to upsert node neighbor: %v", w.cfg.BrokerName, err)
						}
					}
				}
			}
		}
		// record observer neighbors
		// if heard directly (zero-hop), record the observer's own RX SNR
		// of hearing this advertiser as a node_neighbors edge. Skipped if
		// the observer has no node row yet (hasn't advertised itself).
		if packet.PathHashCount() == 0 && (advert.Type() == meshcore.AdvertTypeRepeater || advert.Type() == meshcore.AdvertTypeRoom) {
			observerNodeID, oErr := w.db.GetNodeByPubkey(ctx, observerPubkey)
			if oErr == nil && observerNodeID != nodeID {
				snr := rxSNR
				if err := w.db.UpsertNodeNeighbor(ctx, observerNodeID, nodeID, iata, &snr, nil); err != nil {
					log.Printf("ingest[%s]: failed to upsert observer-advert neighbor: %v", w.cfg.BrokerName, err)
				}
			}
		}
		if err := w.db.UpsertNodeIATA(ctx, nodeID, iata); err != nil {
			log.Printf("ingest[%s]: db: upsert node IATA failed: %v", w.cfg.BrokerName, err)
		}
		if scopeID != nil && (packet.RouteType() == meshcore.RouteTypeTransportFlood || packet.RouteType() == meshcore.RouteTypeTransportDirect) {
			if err := w.db.SetNodeDefaultScope(ctx, nodeID, *scopeID); err != nil {
				log.Printf("ingest[%s]: failed to set default scope for node %s: %v", w.cfg.BrokerName, hex.EncodeToString(advert.PublicKey.PublicKeyBytes()), err)
			}
		}
		prefix4 := advert.PublicKey.PublicKeyBytes()[:4]
		if err := w.db.UpsertNodeShortID(ctx, nodeID, iata, prefix4); err != nil {
			log.Printf("ingest[%s]: failed to upsert node short ID for %s: %v", w.cfg.BrokerName, hex.EncodeToString(prefix4), err)
		}
		pubkeyHex := hex.EncodeToString(advert.PublicKey.PublicKeyBytes())
		isObserver := w.db.IsObserverByPubkey(ctx, advert.PublicKey.PublicKeyBytes())
		var defaultScope *string
		if matchedScope != nil {
			defaultScope = matchedScope
		}
		var radioStr *string
		if radio.FreqMHz != 0 {
			s := fmt.Sprintf("%g,%g,%d", radio.FreqMHz, radio.BWKHz, radio.SF)
			radioStr = &s
		}
		evt := nodeUpdateEvent{
			NodeID:       nodeID.String(),
			PublicKey:    pubkeyHex,
			Name:         advert.AppData().Name,
			NodeType:     advert.Type(),
			NodeTypeName: api.NodeTypeName(int16(advert.Type())),
			IATA:         iata,
			Lat:          lat,
			Lng:          lon,
			IsObserver:   isObserver,
			IATAs:        []api.NodeIATA{{IATA: iata, LastHeard: time.Now().UnixMilli()}},
			DefaultScope: defaultScope,
			Radio:        radioStr,
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

		result, err := DecryptGroupText(ctx, w.db, w.keys, packetHash, packet.Payload)
		if err != nil {
			log.Printf("ingest[%s]: decrypt group text failed: %v", w.cfg.BrokerName, err)
			return
		}
		if result == nil {
			// none of the known keys worked for this hash (or none are known at all);
			// record a hash-only row so the channel is still visible. If a matching key
			// gets added to the config later, BackfillChannelMessages retries this packet
			// at the next boot.
			_, _ = w.db.UpsertChannelHashOnly(ctx, channelHashBytes)
			return
		}

		if result.NewMessage {
			evt := channelMessageEvent{
				ChannelID:   result.ChannelID,
				ChannelHash: hex.EncodeToString(channelHashBytes),
				PacketHash:  hex.EncodeToString(packetHash),
				SenderName:  strings.ReplaceAll(strings.ToValidUTF8(result.Payload.Sender, "\uFFFD"), "\x00", ""),
				Content:     strings.ReplaceAll(strings.ToValidUTF8(result.Payload.Text, "\uFFFD"), "\x00", ""),
				SentAt:      time.Unix(int64(result.Payload.Timestamp), 0).UnixMilli(),
			}
			w.broadcast(hub.EventChannelMessage, iata, 0, fmt.Sprintf("%02x", grpTxt.ChannelHash), evt)
		}
		return
	}
}
