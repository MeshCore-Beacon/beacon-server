// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/json"

	"github.com/google/uuid"
)

// ObserverSummary is the minimal observer representation used in list responses.
type ObserverSummary struct {
	ID           uuid.UUID `json:"id"`
	DisplayName  *string   `json:"displayName,omitempty"`  // friendly name from /status messages
	ObserverType *string   `json:"observerType,omitempty"` // e.g. "meshcoretomqtt", "meshcoreha"
	IATA         string    `json:"iata"`                   // most recently heard IATA
	Status       string    `json:"status"`                 // "online" or "offline" derived from last_status_at
	Radio        *string   `json:"radio,omitempty"`        // friendly radio param string: freqMhz,BwKhz,SF e.g. "910.525,62.5,7"
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
	StatusMetadata   json.RawMessage  `json:"statusMetadata,omitempty" swaggertype:"object"` // raw /status JSON payload
	LastStatusAt     *int64           `json:"lastStatusAt,omitempty"`                        // epoch ms
	FirstSeen        int64            `json:"firstSeen"`                                     // epoch ms
	LastSeen         int64            `json:"lastSeen"`                                      // epoch ms
	ObservationCount int64            `json:"observationCount"`                              // legacy cumulative presence counter; includes non-packet events
	Brokers          []ObserverBroker `json:"brokers"`                                       // broker names this observer has been seen on
}

// ObserverTelemetryPoint is a single telemetry snapshot for an observer.
// Airtime is radio seconds: cumulative since boot on 1h points, per-bucket delta on 6h/24h.
type ObserverTelemetryPoint struct {
	T             int64    `json:"t"` // epoch ms
	BatteryMV     *int32   `json:"batteryMv,omitempty"`
	AirtimeTxSecs *float32 `json:"airtimeTxSecs,omitempty"`
	AirtimeRxSecs *float32 `json:"airtimeRxSecs,omitempty"`
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

// ObserverActivityRadio echoes the observer's current radio params plus the preamble the airtime math assumes.
type ObserverActivityRadio struct {
	FreqMHz         *float32 `json:"freqMhz"`
	SF              int16    `json:"sf"`
	BWKHz           float32  `json:"bwKhz"`
	CR              int16    `json:"cr"`
	PreambleSymbols int      `json:"preambleSymbols"`
}

// ObserverActivityPoint is one bucket of what an observer heard.
type ObserverActivityPoint struct {
	T            int64    `json:"t"` // bucket start, epoch ms
	Observations int64    `json:"observations"`
	AirtimeMs    *float32 `json:"airtimeMs"`
	SNRAvg       *float32 `json:"snrAvg"`
	SNRMin       *float32 `json:"snrMin"`
	RSSIAvg      *float32 `json:"rssiAvg"`
}

// ObserverActivitySummary describes stored packet records, never MQTT presence events.
// Freshness and lastCompleteHour are measured at generatedAt, even with an explicit until;
// only recordedPackets follows the selected activity window.
type ObserverActivitySummary struct {
	RecordedPackets       int64  `json:"recordedPackets"` // stored observations within windowStart/windowEnd
	LastCompleteHour      int64  `json:"lastCompleteHour"`
	LastCompleteHourStart int64  `json:"lastCompleteHourStart"`
	LastCompleteHourEnd   int64  `json:"lastCompleteHourEnd"`
	LatestRecordedAt      *int64 `json:"latestRecordedAt"`
}

// ObserverActivity is the per-observer heard-activity response.
type ObserverActivity struct {
	WindowStart int64                    `json:"windowStart"` // inclusive complete-bucket start, epoch ms
	WindowEnd   int64                    `json:"windowEnd"`   // exclusive end, epoch ms; live requests include the current partial bucket
	GeneratedAt int64                    `json:"generatedAt"` // response computation time, not proof of continuous coverage
	Source      string                   `json:"source"`      // raw or hourly; missing records do not prove an outage
	Summary     *ObserverActivitySummary `json:"summary,omitempty"`
	// Hourly only. Buckets before rolledUntil come from rollups and from rawFrom on from raw rows;
	// when rawFrom is later, the hours between are uncovered, not quiet.
	RolledUntil *int64 `json:"rolledUntil,omitempty"` // end of the newest complete rollup hour, epoch ms
	RawFrom     *int64 `json:"rawFrom,omitempty"`     // start of the raw tail, epoch ms

	Range        string                  `json:"range"`
	Interval     string                  `json:"interval"`
	Radio        *ObserverActivityRadio  `json:"radio"`
	PayloadTypes []PayloadBreakdownItem  `json:"payloadTypes"`
	Points       []ObserverActivityPoint `json:"points"`
}
