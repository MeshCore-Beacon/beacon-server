// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

// AlertRule is one stored rule. Thresholds are in the kind's own unit: hours (gone_quiet), millivolts (battery_low),
// millivolts per day (battery_falling), tenths of a degree C (temperature, low and high), dBm (noise_high), seconds
// (clock), minutes (observer_offline); null for back_online and rebooted. Times are epoch milliseconds.
type AlertRule struct {
	ID             string `json:"id"`
	Kind           string `json:"kind" enums:"gone_quiet,back_online,clock,battery_low,battery_falling,rebooted,temperature,noise_high,observer_offline"`
	Threshold      *int   `json:"threshold" example:"3500"`
	ThresholdHigh  *int   `json:"thresholdHigh"`
	Enabled        bool   `json:"enabled"`
	State          string `json:"state" enums:"ok,firing,inactive,unknown"`
	StateChangedAt int64  `json:"stateChangedAt" example:"1794700000000"`
	LastSentAt     *int64 `json:"lastSentAt" example:"1794703600000"`
}

// AlertTarget is one Atlas pin with the alert kinds its radio can have and its rules.
type AlertTarget struct {
	// ID is the node's or observer's id as used by /nodes/{id} and /observers/{id}; null when the radio no longer exists.
	ID        *string `json:"id" format:"uuid"`
	Kind      string  `json:"kind" enums:"node,observer"`
	PublicKey string  `json:"publicKey"`
	Name      string  `json:"name" example:"Hilltop"`
	// Polled: the radio has a current source: a repeater is assigned to an observer that polls it, an observer has sent
	// status telemetry in the last day. Old readings alone do not count. It is what offers the telemetry kinds and the 3 and 6 hour
	// gone_quiet thresholds for a repeater (observers always have those).
	Polled    bool     `json:"polled"`
	Available []string `json:"available"`
	// Unavailable maps a kind to the English reason this radio cannot have it.
	Unavailable map[string]string `json:"unavailable"`
	// UnavailableCodes maps the same kinds to a stable code for the reason: observer_no_data, repeaters_only, not_polled,
	// no_data, observer_no_clock, clock_repeaters_only.
	UnavailableCodes map[string]string `json:"unavailableCodes"`
	Rules            []AlertRule       `json:"rules"`
}

// AlertsResponse is GET /me/alerts: one target per Atlas pin, in Atlas order.
type AlertsResponse struct {
	Delivery  string        `json:"delivery" enums:"push,email"`
	HasDevice bool          `json:"hasDevice"`
	Targets   []AlertTarget `json:"targets"`
}

// AlertTargetRef names a radio.
type AlertTargetRef struct {
	Kind      string `json:"kind" enums:"node,observer"`
	PublicKey string `json:"publicKey" example:"0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a0f3a"`
}

// PutAlertRuleRequest upserts the rule for (target, kind). Enabled defaults to true when absent.
type PutAlertRuleRequest struct {
	Target        AlertTargetRef `json:"target"`
	Kind          string         `json:"kind"`
	Threshold     *int           `json:"threshold"`
	ThresholdHigh *int           `json:"thresholdHigh"`
	Enabled       *bool          `json:"enabled"`
}

// RegisterDeviceRequest registers the push token of this app session.
type RegisterDeviceRequest struct {
	Platform  string `json:"platform" enums:"ios,android"`
	PushToken string `json:"pushToken"`
}
