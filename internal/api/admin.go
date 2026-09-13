// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

// AdminConfig is a whitelist of selected nonsecret running settings.
// It is not a serialization of the full file or environment configuration.
type AdminConfig struct {
	Auth   AdminAuthConfig   `json:"auth"`
	CORS   AdminCORSConfig   `json:"cors"`
	Ingest AdminIngestConfig `json:"ingest"`
}

type AdminAuthConfig struct {
	Configured bool `json:"configured"`
}

// AdminCORSConfig reports the current options supplied to the CORS middleware,
// with Beacon defaults applied, before that library's matching normalization.
type AdminCORSConfig struct {
	AllowedOrigins   []string `json:"allowed_origins"`
	AllowedMethods   []string `json:"allowed_methods"`
	AllowedHeaders   []string `json:"allowed_headers"`
	AllowCredentials bool     `json:"allow_credentials"`
	MaxAge           int      `json:"max_age"`
}

type AdminIngestConfig struct {
	// BrokerCount counts configured broker workers, not active MQTT connections
	// or a configurable processing-worker pool.
	BrokerCount int `json:"broker_count"`
}

type UpdateAdminConfigRequest struct {
	CORS *UpdateAdminCORSRequest `json:"cors" validate:"required"`
}

type UpdateAdminCORSRequest struct {
	AllowedOrigins *[]string `json:"allowed_origins" validate:"required"`
}

type UpdateAdminConfigResponse struct {
	Config          AdminConfig `json:"config"`
	Persisted       bool        `json:"persisted"`
	RequiresRestart bool        `json:"requires_restart"`
}
