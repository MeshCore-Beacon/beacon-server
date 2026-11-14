// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package polling holds the wire types and parsers for the claim and reading
// messages observer firmware publishes over MQTT.
package polling

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	maxClaimBytes = 16384
	maxClaimNodes = 64
	maxReadings   = 64
	maxLPPBytes   = 160
	maxSlots      = 8
)

type Claim struct {
	Origin          string
	Observer        []byte // 32 bytes, from origin_id
	Timestamp       time.Time
	Client          string // the "plugin" field
	PolicyVersion   string
	Community       bool
	AcceptsRequests bool
	RequestSlots    int
	Nodes           []ClaimNode
}

type ClaimNode struct {
	Target      []byte // 32 bytes
	KeyMode     string // "own", "password", "shared"
	ClaimedSNR  *float32
	Requested   bool
	State       string // "", "pending", "ok", "no_reply", "login_failed", "admin_stop", "not_direct"
	Misses      int
	LastAttempt *time.Time
	LastOK      *time.Time
}

type Reading struct {
	Version      int // 1 or 2
	Observer     []byte
	Client       string
	Target       []byte
	Method       string // "status" or "telemetry"
	SampledAt    time.Time
	Tag          *uint32
	BatteryMV    *int
	TemperatureC *float64
	PollSNR      *float64
	Stats        Stats
	LPP          []byte
	PacketHash   []byte
}

// Stats has every field nil when the reply did not carry it.
type Stats struct {
	UptimeSecs, PacketsRecv, PacketsSent, RecvFlood, RecvDirect, SentFlood, SentDirect,
	TxAirSecs, RxAirSecs, RecvErrors *int64
	NoiseFloor, LastRSSI, TxQueue, FloodDups, DirectDups, ErrEvents *int
	LastSNR                                                         *float64
}

type claimWire struct {
	V               *int            `json:"v"`
	Origin          string          `json:"origin"`
	OriginID        string          `json:"origin_id"`
	Timestamp       string          `json:"timestamp"`
	Plugin          string          `json:"plugin"`
	PolicyVersion   string          `json:"policy_version"`
	Community       bool            `json:"community"`
	AcceptsRequests bool            `json:"accepts_requests"`
	RequestSlots    *int            `json:"request_slots"`
	Nodes           []claimNodeWire `json:"nodes"`
}

type claimNodeWire struct {
	NodeID      string   `json:"node_id"`
	ReplyHops   *int     `json:"reply_hops"`
	Key         string   `json:"key"`
	SNR         *float32 `json:"snr"`
	Requested   bool     `json:"requested"`
	State       string   `json:"state"`
	Misses      *int     `json:"misses"`
	LastAttempt *string  `json:"last_attempt"`
	LastOK      *string  `json:"last_ok"`
}

type readingsWire struct {
	V        *int          `json:"v"`
	OriginID string        `json:"origin_id"`
	Plugin   string        `json:"plugin"`
	Readings []readingWire `json:"readings"`
}

type readingWire struct {
	NodeID       string     `json:"node_id"`
	BatteryMV    *int       `json:"battery_mv"`
	Method       string     `json:"method"`
	SampledAt    string     `json:"sampled_at"`
	SNR          *float64   `json:"snr"`
	TemperatureC *float64   `json:"temperature_c"`
	Stats        *statsWire `json:"stats"`
	Tag          *uint32    `json:"tag"`
	PacketHash   string     `json:"packet_hash"`
	LPP          string     `json:"lpp"`
}

type statsWire struct {
	UptimeSecs  *int64   `json:"uptime_secs"`
	NoiseFloor  *int     `json:"noise_floor"`
	LastRSSI    *int     `json:"last_rssi"`
	LastSNR     *float64 `json:"last_snr"`
	TxQueue     *int     `json:"tx_queue"`
	PacketsRecv *int64   `json:"packets_recv"`
	PacketsSent *int64   `json:"packets_sent"`
	RecvFlood   *int64   `json:"recv_flood"`
	RecvDirect  *int64   `json:"recv_direct"`
	SentFlood   *int64   `json:"sent_flood"`
	SentDirect  *int64   `json:"sent_direct"`
	TxAirSecs   *int64   `json:"tx_air_secs"`
	RxAirSecs   *int64   `json:"rx_air_secs"`
	FloodDups   *int     `json:"flood_dups"`
	DirectDups  *int     `json:"direct_dups"`
	RecvErrors  *int64   `json:"recv_errors"`
	ErrEvents   *int     `json:"err_events"`
}

// cleanText drops NULs and invalid UTF-8, as the ingest package does for its strings.
func cleanText(s string) string {
	return strings.ReplaceAll(strings.ToValidUTF8(s, "�"), "\x00", "")
}

func hexN(name, s string, n int) ([]byte, error) {
	if len(s) != n*2 {
		return nil, fmt.Errorf("%s: want %d hex chars", name, n*2)
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return b, nil
}

func parseTime(name, s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: %w", name, err)
	}
	return t, nil
}

func optTime(name string, s *string) (*time.Time, error) {
	if s == nil {
		return nil, nil
	}
	t, err := parseTime(name, *s)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func validState(s string) bool {
	switch s {
	case "", "pending", "ok", "no_reply", "login_failed", "admin_stop", "not_direct":
		return true
	}
	return false
}

// ParseClaim validates a /polling message.
func ParseClaim(raw []byte) (Claim, error) {
	if len(raw) > maxClaimBytes {
		return Claim{}, errors.New("claim too large")
	}
	var w claimWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return Claim{}, fmt.Errorf("claim json: %w", err)
	}
	if w.V == nil || *w.V != 2 {
		return Claim{}, errors.New("claim: unsupported version")
	}
	obs, err := hexN("origin_id", w.OriginID, 32)
	if err != nil {
		return Claim{}, err
	}
	ts, err := parseTime("timestamp", w.Timestamp)
	if err != nil {
		return Claim{}, err
	}
	if len(w.Nodes) > maxClaimNodes {
		return Claim{}, errors.New("claim: too many nodes")
	}
	c := Claim{
		Origin:          cleanText(w.Origin),
		Observer:        obs,
		Timestamp:       ts,
		Client:          cleanText(w.Plugin),
		PolicyVersion:   cleanText(w.PolicyVersion),
		Community:       w.Community,
		AcceptsRequests: w.AcceptsRequests,
		Nodes:           make([]ClaimNode, 0, len(w.Nodes)),
	}
	if w.RequestSlots != nil {
		if *w.RequestSlots < 0 || *w.RequestSlots > maxSlots {
			return Claim{}, errors.New("claim: request_slots out of range")
		}
		c.RequestSlots = *w.RequestSlots
	}
	for _, nw := range w.Nodes {
		n, err := parseClaimNode(nw)
		if err != nil {
			return Claim{}, err
		}
		c.Nodes = append(c.Nodes, n)
	}
	return c, nil
}

func parseClaimNode(nw claimNodeWire) (ClaimNode, error) {
	target, err := hexN("node_id", nw.NodeID, 32)
	if err != nil {
		return ClaimNode{}, err
	}
	if nw.ReplyHops != nil && *nw.ReplyHops != 0 && *nw.ReplyHops != 1 {
		return ClaimNode{}, errors.New("claim: reply_hops must be 0 or 1")
	}
	switch nw.Key {
	case "own", "password", "shared":
	default:
		return ClaimNode{}, errors.New("claim: unknown key mode")
	}
	if !validState(nw.State) {
		return ClaimNode{}, errors.New("claim: unknown state")
	}
	n := ClaimNode{Target: target, KeyMode: nw.Key, Requested: nw.Requested, State: nw.State}
	if nw.SNR != nil {
		if math.IsNaN(float64(*nw.SNR)) || math.IsInf(float64(*nw.SNR), 0) {
			return ClaimNode{}, errors.New("claim: bad snr")
		}
		n.ClaimedSNR = nw.SNR
	}
	if nw.Misses != nil {
		if *nw.Misses < 0 {
			return ClaimNode{}, errors.New("claim: negative misses")
		}
		n.Misses = *nw.Misses
	}
	if n.LastAttempt, err = optTime("last_attempt", nw.LastAttempt); err != nil {
		return ClaimNode{}, err
	}
	if n.LastOK, err = optTime("last_ok", nw.LastOK); err != nil {
		return ClaimNode{}, err
	}
	return n, nil
}

// ParseReadings validates a /telemetry or /battery message and returns one
// Reading per entry.
func ParseReadings(raw []byte) ([]Reading, error) {
	if len(raw) > maxClaimBytes {
		return nil, errors.New("readings too large")
	}
	var w readingsWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, fmt.Errorf("readings json: %w", err)
	}
	if w.V == nil || (*w.V != 1 && *w.V != 2) {
		return nil, errors.New("readings: unsupported version")
	}
	obs, err := hexN("origin_id", w.OriginID, 32)
	if err != nil {
		return nil, err
	}
	if len(w.Readings) == 0 || len(w.Readings) > maxReadings {
		return nil, errors.New("readings: bad entry count")
	}
	client := cleanText(w.Plugin)
	out := make([]Reading, 0, len(w.Readings))
	for _, rw := range w.Readings {
		r, err := parseReading(rw, *w.V, obs, client)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func parseReading(rw readingWire, ver int, obs []byte, client string) (Reading, error) {
	target, err := hexN("node_id", rw.NodeID, 32)
	if err != nil {
		return Reading{}, err
	}
	if rw.Method != "status" && rw.Method != "telemetry" {
		return Reading{}, errors.New("readings: unknown method")
	}
	if rw.SampledAt == "" {
		return Reading{}, errors.New("readings: sampled_at missing")
	}
	at, err := parseTime("sampled_at", rw.SampledAt)
	if err != nil {
		return Reading{}, err
	}
	r := Reading{
		Version: ver, Observer: obs, Client: client, Target: target,
		Method: rw.Method, SampledAt: at, Tag: rw.Tag, PollSNR: finite(rw.SNR),
	}
	if rw.PacketHash != "" {
		if r.PacketHash, err = hex.DecodeString(rw.PacketHash); err != nil {
			return Reading{}, fmt.Errorf("packet_hash: %w", err)
		}
	}
	if len(rw.LPP) > maxLPPBytes*2 {
		return Reading{}, errors.New("readings: lpp too long")
	}
	if rw.LPP != "" {
		if r.LPP, err = hex.DecodeString(rw.LPP); err != nil {
			return Reading{}, fmt.Errorf("lpp: %w", err)
		}
	}
	// Zero means "no voltage" on the wire (v1 always sends the field).
	if rw.BatteryMV != nil && *rw.BatteryMV > 0 && *rw.BatteryMV <= 65535 {
		r.BatteryMV = rw.BatteryMV
	}
	if t := finite(rw.TemperatureC); t != nil && *t >= lppTempMin/10 && *t <= lppTempMax/10 {
		r.TemperatureC = t
	}
	if len(r.LPP) > 0 {
		mv, c := DecodeLPP(r.LPP)
		if r.BatteryMV == nil && mv != nil && *mv > 0 {
			r.BatteryMV = mv
		}
		if r.TemperatureC == nil {
			r.TemperatureC = c
		}
	}
	if rw.Stats != nil {
		r.Stats = convertStats(rw.Stats)
	}
	return r, nil
}

func finite(f *float64) *float64 {
	if f == nil || math.IsNaN(*f) || math.IsInf(*f, 0) {
		return nil
	}
	return f
}

func convertStats(s *statsWire) Stats {
	return Stats{
		UptimeSecs: s.UptimeSecs, PacketsRecv: s.PacketsRecv, PacketsSent: s.PacketsSent,
		RecvFlood: s.RecvFlood, RecvDirect: s.RecvDirect, SentFlood: s.SentFlood,
		SentDirect: s.SentDirect, TxAirSecs: s.TxAirSecs, RxAirSecs: s.RxAirSecs,
		RecvErrors: s.RecvErrors, NoiseFloor: s.NoiseFloor, LastRSSI: s.LastRSSI,
		TxQueue: s.TxQueue, FloodDups: s.FloodDups, DirectDups: s.DirectDups,
		ErrEvents: s.ErrEvents, LastSNR: finite(s.LastSNR),
	}
}
