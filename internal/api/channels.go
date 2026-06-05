package api

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
