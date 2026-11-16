// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package api

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const MaxRouteEvidenceWindow = 30 * 24 * time.Hour

var ErrRouteEvidenceInput = errors.New("invalid route evidence request")

// RouteObservation is a retained report reference, not a packet body or proof of hop identity.
type RouteObservation struct {
	ID              int64     `json:"id"`
	PacketHash      string    `json:"packetHash"`
	ObserverID      uuid.UUID `json:"observerId"`
	ObserverName    *string   `json:"observerName,omitempty"`
	HeardAt         int64     `json:"heardAt"`
	PayloadType     int16     `json:"payloadType"`
	PayloadTypeName string    `json:"payloadTypeName"`
	RSSI            *int16    `json:"rssi,omitempty"`
	SNR             *float32  `json:"snr,omitempty"`
}

// RouteEvidence pages by nextPageCursor, which keeps full timestamp precision.
type RouteEvidence struct {
	Items          []RouteObservation `json:"items"`
	HasMore        bool               `json:"hasMore"`
	Route          KnownRoute         `json:"route"`
	WindowStart    int64              `json:"windowStart"`
	WindowEnd      int64              `json:"windowEnd"`
	GeneratedAt    int64              `json:"generatedAt"`
	MatchType      string             `json:"matchType"`
	MatchAvailable bool               `json:"matchAvailable"`
	HashSize       int16              `json:"hashSize"`
	PathBytes      string             `json:"pathBytes"`
	NextPageCursor *string            `json:"nextPageCursor,omitempty"`
}

type RouteEvidenceQuery struct {
	Since  time.Time
	Until  time.Time
	Cursor *RouteEvidenceCursor
	Limit  int32
	// Optional exact representation copied from a prior response or cursor.
	HashSize  int16
	PathBytes string
}

// RouteEvidenceCursor pins the route, window and exact representation as well as
// the precise (heard_at DESC,id DESC) boundary. Legacy v1 cursors have no path pin.
type RouteEvidenceCursor struct {
	IATA      string
	PathKey   string
	Since     time.Time
	Until     time.Time
	HeardAt   time.Time
	ID        int64
	HashSize  int16
	PathBytes string
}

func ValidRouteEvidenceKey(iata, key string) bool {
	if len(iata) != 3 || len(key) != 32 {
		return false
	}
	for _, c := range iata {
		if !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') {
			return false
		}
	}
	_, err := hex.DecodeString(key)
	return err == nil && key == strings.ToLower(key)
}

func ValidRouteEvidenceWindow(since, until time.Time) bool {
	return !since.Before(time.Unix(0, 0)) && until.After(since) && until.Sub(since) <= MaxRouteEvidenceWindow
}

func ValidRouteEvidencePath(width int16, path string) bool {
	if width < 1 || width > 3 || len(path) < 4*int(width) || len(path) > 126*int(width) || len(path)%(2*int(width)) != 0 {
		return false
	}
	_, err := hex.DecodeString(path)
	return err == nil && path == strings.ToLower(path)
}

func (c RouteEvidenceCursor) String() string {
	value := fmt.Sprintf("%s:%s:%d:%d:%d:%d", c.IATA, c.PathKey, c.Since.UnixMilli(), c.Until.UnixMilli(), c.HeardAt.UnixMicro(), c.ID)
	if c.HashSize != 0 || c.PathBytes != "" {
		return fmt.Sprintf("v2:%s:%d:%s", value, c.HashSize, c.PathBytes)
	}
	return "v1:" + value
}

func ParseRouteEvidenceCursor(raw string) (*RouteEvidenceCursor, error) {
	if len(raw) > 512 {
		return nil, ErrRouteEvidenceInput
	}
	p := strings.Split(raw, ":")
	if !((len(p) == 7 && p[0] == "v1") || (len(p) == 9 && p[0] == "v2")) || !ValidRouteEvidenceKey(p[1], p[2]) {
		return nil, ErrRouteEvidenceInput
	}
	values := [4]int64{}
	for i, s := range p[3:7] {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 0 || strconv.FormatInt(n, 10) != s {
			return nil, ErrRouteEvidenceInput
		}
		values[i] = n
	}
	// Bound before converting; this also keeps millisecond/microsecond comparisons safe.
	if values[0] > 253402300799999 || values[1] > 253402300799999 || values[2] > 253402300799999999 || values[3] <= 0 {
		return nil, ErrRouteEvidenceInput
	}
	c := &RouteEvidenceCursor{IATA: p[1], PathKey: p[2], Since: time.UnixMilli(values[0]).UTC(), Until: time.UnixMilli(values[1]).UTC(), HeardAt: time.UnixMicro(values[2]).UTC(), ID: values[3]}
	if p[0] == "v2" {
		width, err := strconv.ParseInt(p[7], 10, 16)
		if err != nil || strconv.FormatInt(width, 10) != p[7] || !ValidRouteEvidencePath(int16(width), p[8]) {
			return nil, ErrRouteEvidenceInput
		}
		c.HashSize, c.PathBytes = int16(width), p[8]
	}
	if !ValidRouteEvidenceWindow(c.Since, c.Until) || c.HeardAt.Before(c.Since) || !c.HeardAt.Before(c.Until) {
		return nil, ErrRouteEvidenceInput
	}
	return c, nil
}
