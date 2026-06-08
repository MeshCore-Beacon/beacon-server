// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import "github.com/google/uuid"

// ObserverSummary is the minimal observer representation used in list responses.
type ObserverSummary struct {
	ID           uuid.UUID `json:"id"`
	DisplayName  *string   `json:"displayName,omitempty"`  // friendly name from /status messages
	ObserverType *string   `json:"observerType,omitempty"` // e.g. "meshcoretomqtt", "meshcoreha"
	IATA         string    `json:"iata"`                   // most recently heard IATA
	Status       string    `json:"status"`                 // "online" or "offline" derived from last_status_at
	Radio        *string   `json:"radio,omitempty"`        // friendly radio param string: freqMhz,BwKhz,SF
	Scopes       []string  `json:"scopes,omitempty"`       // list of observer forwarded scopes matched to config
}

// ObserverBroker represents a single MQTT broker an observer has been seen on,
// including timestamps for diagnosing partial outages — e.g. distinguishing
// "observer is down" from "one broker stopped delivering for this observer".
type ObserverBroker struct {
	Name         string `json:"name"`         // broker name e.g. "mqtt1"
	LastSeenAt   int64  `json:"lastSeenAt"`   // epoch ms, last time observer was seen on this broker
	LastPacketAt int64  `json:"lastPacketAt"` // epoch ms, last packet received via this broker; 0 if none
}

// Observer is the full observer representation including radio config,
// telemetry, broker memberships and raw status metadata.
type Observer struct {
	ObserverSummary
	PublicKey        string           `json:"publicKey"` // hex-encoded public key
	SoftwareVersion  *string          `json:"softwareVersion,omitempty"`
	HardwareModel    *string          `json:"hardwareModel,omitempty"`
	FirmwareVersion  *string          `json:"firmwareVersion,omitempty"`
	FirmwareBuild    *string          `json:"firmwareBuild,omitempty"`
	RadioFreqMHz     *float32         `json:"radioFreqMhz,omitempty"` // MHz e.g. 910.525
	RadioSF          *int16           `json:"radioSf,omitempty"`      // LoRa spreading factor
	RadioBWKHz       *float32         `json:"radioBwKhz,omitempty"`   // bandwidth in kHz
	RadioCR          *int16           `json:"radioCr,omitempty"`      // coding rate denominator
	BatteryLevel     *float32         `json:"batteryLevel,omitempty"` // volts, nil if mains powered
	UptimeSeconds    *int64           `json:"uptimeSeconds,omitempty"`
	StatusMetadata   any              `json:"statusMetadata,omitempty"` // raw /status JSON payload
	LastStatusAt     *int64           `json:"lastStatusAt,omitempty"`   // epoch ms
	FirstSeen        int64            `json:"firstSeen"`                // epoch ms
	LastSeen         int64            `json:"lastSeen"`                 // epoch ms
	ObservationCount int64            `json:"observationCount"`
	Brokers          []ObserverBroker `json:"brokers"` // broker names this observer has been seen on
}

// ObserverTelemetryPoint is a single telemetry snapshot for an observer.
type ObserverTelemetryPoint struct {
	T             int64    `json:"t"` // epoch ms
	BatteryMV     *int32   `json:"batteryMv,omitempty"`
	AirtimeTxPct  *float32 `json:"airtimeTxPct,omitempty"`
	AirtimeRxPct  *float32 `json:"airtimeRxPct,omitempty"`
	NoiseFloorDB  *float32 `json:"noiseFloorDb,omitempty"`
	UptimeSeconds *int64   `json:"uptimeSeconds,omitempty"`
	QueueLength   *int32   `json:"queueLength,omitempty"`
	ReceiveErrors *int32   `json:"receiveErrors,omitempty"`
}

// ObserverTelemetry is the full telemetry response for an observer.
// Range and interval reflect the query parameters used.
type ObserverTelemetry struct {
	Range    string                   `json:"range"`
	Interval string                   `json:"interval"`
	Points   []ObserverTelemetryPoint `json:"points"`
}
