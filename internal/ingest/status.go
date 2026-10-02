// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/MeshCore-Beacon/beacon-server/internal/lora"
)

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

// statusEvent is the JSON payload for an observerStatus WS event.
// Shape matches the design doc § Server → Client events.
type statusEvent struct {
	ObserverID    string   `json:"observerId"`
	DisplayName   string   `json:"displayName"`
	ObserverType  *string  `json:"observerType,omitempty"`
	IATA          string   `json:"iata,omitempty"`
	Online        bool     `json:"online"`
	Radio         *string  `json:"radio,omitempty"`
	Scopes        []string `json:"scopes"`
	BatteryMV     int      `json:"batteryMv,omitempty"`
	UptimeSeconds int64    `json:"uptimeSeconds"`
	LastStatusAt  int64    `json:"lastStatusAt"`
}

type statusStats struct {
	UptimeSeconds int64   `json:"uptime_secs"`
	BatteryMV     int     `json:"battery_mv"`
	NoiseFloor    float32 `json:"noise_floor"`
	QueueLen      int     `json:"queue_len"`
	DebugFlags    int     `json:"debug_flags"`
	TxAirSecs     float64 `json:"tx_air_secs"`
	RxAirSecs     float64 `json:"rx_air_secs"`
	RecvErrors    int     `json:"recv_errors"`
}

// usable reports whether the stats carry real radio readings. A running observer
// never reports zero uptime, and a live radio never reads a 0 dB noise floor, so
// that with zero airtime marks a status sent before the radio stats were filled in.
func (s statusStats) usable() bool {
	if s.UptimeSeconds == 0 {
		return false
	}
	return s.NoiseFloor != 0 || s.TxAirSecs != 0 || s.RxAirSecs != 0
}

// LoRa radios span 137 MHz sub-GHz to 2.5 GHz SX128x.
const (
	minRadioFreqMHz = 100
	maxRadioFreqMHz = 3000
)

// parseRadioFloat rejects NaN, Inf and anything outside [lo, hi]; observers send free text and
// one bad value would poison every airtime the observer is later costed with.
func parseRadioFloat(s string, lo, hi float64) (float64, error) {
	f, err := strconv.ParseFloat(s, 32)
	if err != nil {
		return 0, err
	}
	if !(f >= lo && f <= hi) {
		return 0, fmt.Errorf("radio value %q outside %g..%g", s, lo, hi)
	}
	return f, nil
}

// stripNULs removes NUL bytes, which Postgres text columns reject, and returns the
// names of the fields that had any.
func stripNULs(fields map[string]*string) []string {
	var dirty []string
	for name, s := range fields {
		if strings.Contains(*s, "\x00") {
			*s = strings.ReplaceAll(*s, "\x00", "")
			dirty = append(dirty, name)
		}
	}
	slices.Sort(dirty)
	return dirty
}

// cleanText makes over-the-air text storable: invalid UTF-8 replaced, NULs dropped.
func cleanText(s string) string {
	return strings.ReplaceAll(strings.ToValidUTF8(s, "�"), "\x00", "")
}

// stripJSONNULs drops \u0000 from a JSON document's strings, which jsonb rejects.
// Payloads without one are returned untouched.
func stripJSONNULs(raw []byte) ([]byte, bool) {
	if !bytes.Contains(raw, []byte(`\u0000`)) {
		return raw, false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return raw, false
	}
	out, err := json.Marshal(dropNULs(v))
	if err != nil {
		return raw, false
	}
	return out, true
}

func dropNULs(v any) any {
	switch v := v.(type) {
	case string:
		return strings.ReplaceAll(v, "\x00", "")
	case []any:
		for i := range v {
			v[i] = dropNULs(v[i])
		}
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, e := range v {
			out[strings.ReplaceAll(k, "\x00", "")] = dropNULs(e)
		}
		return out
	}
	return v
}

// handleStatus processes a /status message and fans out an observerStatus event.
func (w *Worker) handleStatus(ctx context.Context, pubkeyHex string, raw []byte) {
	var envelope struct {
		ObserverType    string      `json:"source"`
		SoftwareVersion string      `json:"client_version"`
		HardwareModel   string      `json:"model"`
		FirmwareVersion string      `json:"firmware_version"`
		DisplayName     string      `json:"origin"`
		RadioString     string      `json:"radio"`
		Stats           statusStats `json:"stats"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		w.log.Warn(fmt.Sprintf("malformed status envelope from %s", pubkeyHex), "error", err)
		return
	}
	pubkey, err := hex.DecodeString(pubkeyHex)
	if err != nil {
		w.log.Warn(fmt.Sprintf("invalid pubkey hex in status from %s", pubkeyHex), "error", err)
		return
	}
	dirty := stripNULs(map[string]*string{
		"source": &envelope.ObserverType, "client_version": &envelope.SoftwareVersion,
		"model": &envelope.HardwareModel, "firmware_version": &envelope.FirmwareVersion,
		"origin": &envelope.DisplayName, "radio": &envelope.RadioString,
	})
	raw, rawDirty := stripJSONNULs(raw)
	if len(dirty) > 0 || rawDirty {
		w.log.Warn(fmt.Sprintf("stripped NUL bytes from status from %s", pubkeyHex), "fields", dirty, "metadata", rawDirty)
	}
	id, _, err := w.db.UpsertObserver(ctx, pubkey, "")
	if err != nil {
		w.log.Error(fmt.Sprintf("db: upsert observer failed in status from %s", pubkeyHex), "error", err)
		return
	}
	// invalidate cache for observer details
	if w.onObserverUpsert != nil {
		w.onObserverUpsert(ctx, id)
	}
	if err := w.db.UpsertObserverBroker(ctx, id, w.cfg.BrokerName, false); err != nil {
		w.log.Error(fmt.Sprintf("db: upsert observer broker failed in status from %s", pubkeyHex), "error", err)
	}
	params := UpdateObserverStatusParams{
		PublicKey:      pubkey,
		StatusMetadata: raw,
		LastStatusAt:   time.Now(),
	}
	// A running observer never reports 0, so treat this the same as a missing
	// stats object and leave the existing value alone (COALESCE in the SQL)
	// rather than stomping it with a zero.
	if envelope.Stats.UptimeSeconds != 0 {
		params.UptimeSeconds = &envelope.Stats.UptimeSeconds
	}
	if envelope.Stats.BatteryMV != 0 {
		batteryLevel := float32(envelope.Stats.BatteryMV) / 1000
		params.BatteryLevel = &batteryLevel
	}
	if envelope.SoftwareVersion != "" {
		params.SoftwareVersion = envelope.SoftwareVersion
	}
	params.ObserverType = inferObserverType(envelope.ObserverType, envelope.SoftwareVersion)
	if envelope.DisplayName != "" {
		params.DisplayName = strings.ToValidUTF8(envelope.DisplayName, "\uFFFD")
	}
	if envelope.HardwareModel != "" {
		params.HardwareModel = envelope.HardwareModel
	}
	if envelope.FirmwareVersion != "" {
		params.FirmwareVersion = envelope.FirmwareVersion
	}

	radio := strings.Split(strings.TrimSpace(envelope.RadioString), ",")
	if len(radio) != 4 {
		w.log.Warn(fmt.Sprintf("missing or malformed radio params in status from %s, skipping radio fields", pubkeyHex))
	} else {
		freq, err := parseRadioFloat(radio[0], minRadioFreqMHz, maxRadioFreqMHz)
		if err != nil {
			w.log.Warn(fmt.Sprintf("error parsing radio freq in status from %s", pubkeyHex), "error", err)
		} else {
			params.RadioFreqMHz = float32(freq)
		}
		bw, err := parseRadioFloat(radio[1], lora.MinBWKHz, lora.MaxBWKHz)
		if err != nil {
			w.log.Warn(fmt.Sprintf("error parsing radio bw in status from %s", pubkeyHex), "error", err)
		} else {
			params.RadioBWKHz = float32(bw)
		}
		sf, err := strconv.ParseInt(radio[2], 10, 16)
		if err != nil {
			w.log.Warn(fmt.Sprintf("error parsing radio sf in status from %s", pubkeyHex), "error", err)
		} else {
			params.RadioSF = int16(sf)
		}
		cr, err := strconv.ParseInt(radio[3], 10, 16)
		if err != nil {
			w.log.Warn(fmt.Sprintf("error parsing radio cr in status from %s", pubkeyHex), "error", err)
		} else {
			params.RadioCR = int16(cr)
		}
	}

	observerID, err := w.db.UpdateObserverStatus(ctx, params)
	if err != nil {
		w.log.Error(fmt.Sprintf("db: update observer status failed for %s", pubkeyHex), "error", err)
		return
	}
	// Store a telemetry snapshot at the configured resolution. Partial stats are
	// skipped rather than written as zeros that would win the hourly dedup and
	// pollute the telemetry aggregates.
	if !envelope.Stats.usable() {
		w.log.Debug("status has no usable stats; skipping telemetry insert")
	} else {
		resolution := w.cfg.TelemetryResolution
		if resolution == 0 {
			resolution = time.Hour
		}
		reportedAt := time.Now().Truncate(resolution)
		batteryMV := int32(envelope.Stats.BatteryMV)
		txAirSecs := float32(envelope.Stats.TxAirSecs)
		rxAirSecs := float32(envelope.Stats.RxAirSecs)
		queueLen := int32(envelope.Stats.QueueLen)
		debugFlags := int32(envelope.Stats.DebugFlags)
		recvErrors := int32(envelope.Stats.RecvErrors)

		if err := w.db.InsertObserverTelemetry(
			ctx, observerID, reportedAt, &batteryMV, &txAirSecs, &rxAirSecs,
			envelope.Stats.NoiseFloor, envelope.Stats.UptimeSeconds,
			&queueLen, &debugFlags, &recvErrors,
		); err != nil {
			w.log.Error(fmt.Sprintf("db: insert telemetry failed for %s", pubkeyHex), "error", err)
		}
	}

	iata, err := w.db.GetObserverLastIATA(ctx, observerID)
	if err != nil {
		iata = "" // non-fatal, continue
	}
	scopes, err := w.db.GetObserverScopes(ctx, observerID)
	if err != nil {
		w.log.Error(fmt.Sprintf("failed to get observer scopes for %s", pubkeyHex), "error", err)
		scopes = []string{}
	}
	var radioStr *string
	if params.RadioFreqMHz != 0 {
		s := fmt.Sprintf("%g,%g,%d", params.RadioFreqMHz, params.RadioBWKHz, params.RadioSF)
		radioStr = &s
	}
	var observerType *string
	if envelope.ObserverType != "" {
		observerType = &envelope.ObserverType
	}
	evt := statusEvent{
		ObserverID:    observerID.String(),
		DisplayName:   envelope.DisplayName,
		ObserverType:  observerType,
		IATA:          iata,
		Online:        true,
		Radio:         radioStr,
		Scopes:        scopes,
		BatteryMV:     envelope.Stats.BatteryMV,
		UptimeSeconds: envelope.Stats.UptimeSeconds,
		LastStatusAt:  time.Now().UnixMilli(),
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		w.log.Error(fmt.Sprintf("failed to marshal status event payload for %s", pubkeyHex), "error", err)
		return
	}
	w.hub.Broadcast(hub.Event{Type: hub.EventObserverStatus, Payload: payload, IATA: iata, ObserverID: evt.ObserverID})
}
