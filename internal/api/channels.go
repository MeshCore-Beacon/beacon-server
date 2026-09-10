// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ChannelPage adds a precise cursor while retaining the numeric cursor for older clients.
type ChannelPage struct {
	Page[ChannelSummary]
	NextPageCursor *string `json:"nextPageCursor,omitempty"`
}

// ChannelCursor identifies a boundary in (last_seen DESC, id DESC) order.
type ChannelCursor struct {
	LastSeen time.Time
	ID       int32
}

func (c ChannelCursor) String() string {
	return fmt.Sprintf("v1:%d:%d", c.LastSeen.UnixMicro(), c.ID)
}

var errChannelCursor = errors.New("invalid channel page cursor")

// ParseChannelCursor parses the opaque, versioned cursor returned by ChannelPage.
func ParseChannelCursor(raw string) (*ChannelCursor, error) {
	if len(raw) > 64 {
		return nil, errChannelCursor
	}
	parts := strings.Split(raw, ":")
	if len(parts) != 3 || parts[0] != "v1" {
		return nil, errChannelCursor
	}
	micros, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, errChannelCursor
	}
	id, err := strconv.ParseInt(parts[2], 10, 32)
	if err != nil || id <= 0 {
		return nil, errChannelCursor
	}
	at := time.UnixMicro(micros).UTC()
	if at.Year() < 1 || at.Year() > 9999 {
		return nil, errChannelCursor
	}
	return &ChannelCursor{LastSeen: at, ID: int32(id)}, nil
}

// ChannelMessage represents a single decrypted channel message.
// Only messages for channels with a known key are stored and returned.
type ChannelMessage struct {
	ID               int64  `json:"id"`
	PacketHash       string `json:"packetHash"`       // hex-encoded packet hash for correlation with packet events
	ChannelHash      string `json:"channelHash"`      // hex-encoded single-byte channel hash
	SenderName       string `json:"senderName"`       // display name from the decrypted payload
	Content          string `json:"content"`          // decrypted message text
	SentAt           int64  `json:"sentAt"`           // epoch ms, from the sender's embedded timestamp
	ObservationCount int64  `json:"observationCount"` // number of packet_observations rows for this message's packet hash
}

// ChannelSummary is the minimal channel representation used in list responses.
type ChannelSummary struct {
	ID          int     `json:"id"`
	Name        *string `json:"name,omitempty"` // display name from config or nil
	ChannelHash string  `json:"channelHash"`    // hex-encoded single-byte hash
	LastSeen    int64   `json:"lastSeen"`       // epoch ms, time of most recent message
	IsHashtag   bool    `json:"isHashtag"`      // true if key was derived from a hashtag PSK
	KeyKnown    bool    `json:"keyKnown"`       // true if Beacon has a decryption key for this channel
}

// Channel is the full channel representation including decryption metadata.
// KeyFingerprint is only populated for hashtag channels since their keys are
// publicly derivable from the tag name.
type Channel struct {
	ChannelSummary
	Hashtag        *string `json:"hashtag,omitempty"`        // tag name without # prefix; non-nil only for hashtag channels
	KeyFingerprint *string `json:"keyFingerprint,omitempty"` // first 8 bytes of SHA256(key), hex-encoded
	MessageCount   int64   `json:"messageCount"`
}
