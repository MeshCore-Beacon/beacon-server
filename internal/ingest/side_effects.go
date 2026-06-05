package ingest

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
	"github.com/meshcore-go/meshcore-go"
)

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

// handlePayloadTypeSideEffects runs payload-type-specific processing after a
// new observation is confirmed inserted. Currently handles:
//   - PayloadTypeAdvert (0x04): upsert node and node_iatas
//   - PayloadTypeGrpTxt (0x05): decrypt and store channel message if key is known
func (w *Worker) handlePayloadTypeSideEffects(ctx context.Context, packet *meshcore.Packet, iata string, packetHash []byte, radio RadioSettings, scopeID *int32) {
	if packet.PayloadType() == meshcore.PayloadTypeAdvert {
		advert, err := meshcore.AdvertFromBytes(packet.Payload)
		if err != nil {
			log.Printf("ingest[%s]: error decoding advert payload: %v", w.cfg.BrokerName, err)
			return
		}
		var lat, lon *float64
		if advert.AppData().Lat != 0 || advert.AppData().Lon != 0 {
			la := float64(advert.AppData().Lat) / 1e6
			lo := float64(advert.AppData().Lon) / 1e6
			lat = &la
			lon = &lo
		}
		params := UpsertNodeParams{
			PublicKey: advert.PublicKey.PublicKeyBytes(),
			Name:      strings.ToValidUTF8(advert.AppData().Name, "\uFFFD"),
			NodeType:  advert.Type(),
			Latitude:  lat,
			Longitude: lon,
		}
		nodeID, err := w.db.UpsertNode(ctx, params, radio)
		if err != nil {
			log.Printf("ingest[%s]: db: upsert node failed: %v", w.cfg.BrokerName, err)
			return
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
			SenderName: strings.ReplaceAll(strings.ToValidUTF8(payload.Sender, "\uFFFD"), "\x00", ""),
			SentAt:     time.Unix(int64(payload.Timestamp), 0),
			Content:    strings.ReplaceAll(strings.ToValidUTF8(payload.Text, "\uFFFD"), "\x00", ""),
		}
		newMsg, err := w.db.InsertChannelMessage(ctx, params)
		if err != nil {
			log.Printf("ingest[%s]: db: insert channel message failed: %v", w.cfg.BrokerName, err)
			return
		}

		if newMsg {
			if err := w.db.SetPacketDecrypted(ctx, packetHash[:]); err != nil {
				log.Printf("ingest[%s]: failed to set packet decrypted: %v", w.cfg.BrokerName, err)
			}
			evt := channelMessageEvent{
				ChannelID:   channelID,
				ChannelHash: hex.EncodeToString(channelHashBytes),
				PacketHash:  hex.EncodeToString(packetHash),
				SenderName:  strings.ReplaceAll(strings.ToValidUTF8(payload.Sender, "\uFFFD"), "\x00", ""),
				Content:     strings.ReplaceAll(strings.ToValidUTF8(payload.Text, "\uFFFD"), "\x00", ""),
				SentAt:      time.Unix(int64(payload.Timestamp), 0).UnixMilli(),
			}
			w.broadcast(hub.EventChannelMessage, iata, 0, fmt.Sprintf("%02x", grpTxt.ChannelHash), evt)
		}
		return
	}
}
