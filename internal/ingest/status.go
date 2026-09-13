// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
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
		w.log.Warn(fmt.Sprintf("malformed status envelope from %s", pubkeyHex), "error", err)
		return
	}
	pubkey, err := hex.DecodeString(pubkeyHex)
	if err != nil {
		w.log.Warn(fmt.Sprintf("invalid pubkey hex in status from %s", pubkeyHex), "error", err)
		return
	}
	id, _, err := w.db.UpsertObserver(ctx, pubkey)
	if err != nil {
		w.log.Error(fmt.Sprintf("db: upsert observer failed in status from %s", pubkeyHex), "error", err)
		return
	}
	// invalidate cache for observer details
	if w.onObserverUpsert != nil {
		w.onObserverUpsert(ctx, id)
	}
	if err := w.db.UpsertObserverBroker(ctx, id, w.cfg.BrokerName); err != nil {
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
		freq, err := strconv.ParseFloat(radio[0], 32)
		if err != nil {
			w.log.Warn(fmt.Sprintf("error parsing radio freq in status from %s", pubkeyHex), "error", err)
		} else {
			params.RadioFreqMHz = float32(freq)
		}
		bw, err := strconv.ParseFloat(radio[1], 32)
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
	// Store a telemetry snapshot at the configured resolution.
	// A running observer never reports uptime_secs == 0, so its absence (or a
	// missing/renamed "stats" object entirely) means this payload has no usable
	// stats — skip the insert rather than writing an all-zero row that would win
	// the hourly dedup and pollute the telemetry aggregates.
	if envelope.Stats.UptimeSeconds == 0 {
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
	w.hub.Broadcast(hub.Event{Type: hub.EventObserverStatus, Payload: payload, IATA: iata})
}
