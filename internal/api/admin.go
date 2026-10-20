// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"errors"
	"time"
)

// AdminConfig is a whitelist of selected nonsecret running settings.
// It is not a serialization of the full file or environment configuration.
type AdminConfig struct {
	Auth   AdminAuthConfig    `json:"auth"`
	CORS   AdminCORSConfig    `json:"cors"`
	Ingest AdminIngestConfig  `json:"ingest"`
	Mobile AdminMobileConfig  `json:"mobile"`
	Web    AdminWebConfig     `json:"web"`
	Guards *AdminGuardsConfig `json:"guards,omitempty"` // absent when database guards are not configured
}

// AdminGuardsConfig reports the database cost guards.
type AdminGuardsConfig struct {
	Enforce       []string `json:"enforce"`         // families currently enforced; the configuration file is the boot default
	ObserveOnly   bool     `json:"observe_only"`    // the single plan layout makes every family observe
	Budget        string   `json:"budget"`          // always "shadow"
	APIPlanLayout string   `json:"api_plan_layout"` // custom_all, split or single
}

type AdminMobileConfig struct {
	MinAppVersion string `json:"min_app_version"` // effective floor; empty means no requirement
}

type AdminWebConfig struct {
	MinWebVersion string `json:"min_web_version"` // effective floor; empty means no requirement
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

// UpdateAdminConfigRequest carries cors, guards or both; at least one is required.
type UpdateAdminConfigRequest struct {
	CORS   *UpdateAdminCORSRequest   `json:"cors"`
	Guards *UpdateAdminGuardsRequest `json:"guards"`
}

type UpdateAdminCORSRequest struct {
	AllowedOrigins *[]string `json:"allowed_origins" validate:"required"`
}

// UpdateAdminGuardsRequest replaces the enforced guard families; an empty list makes every family observe.
type UpdateAdminGuardsRequest struct {
	Enforce *[]string `json:"enforce" validate:"required"`
}

type UpdateAdminConfigResponse struct {
	Config          AdminConfig `json:"config"`
	Persisted       bool        `json:"persisted"`
	RequiresRestart bool        `json:"requires_restart"`
}

// ErrStatementsUnavailable means the pg_stat_statements extension is missing or not preloaded.
var ErrStatementsUnavailable = errors.New("pg_stat_statements is not available")

// DBStatement sums pg_stat_statements rows for one sqlc statement in the current database.
type DBStatement struct {
	Name       string  `json:"name"` // sqlc statement name; "(unnamed)" aggregates the rest
	Calls      int64   `json:"calls"`
	ExecMs     float64 `json:"exec_ms"`
	PlanMs     float64 `json:"plan_ms"` // 0 unless pg_stat_statements.track_planning is on
	SharedHit  int64   `json:"shared_hit"`
	SharedRead int64   `json:"shared_read"`
	Rows       int64   `json:"rows"`
	WALBytes   int64   `json:"wal_bytes"`
}

// DBStatementsResponse lists statements by total execution time, highest first.
type DBStatementsResponse struct {
	Items []DBStatement `json:"items"`
}

// ErrPlanguardNotRun means the daily plan check has not finished a run since the server started.
var ErrPlanguardNotRun = errors.New("planguard has not run yet")

// ErrPlanguardDisabled means planguard.daily_check is off.
var ErrPlanguardDisabled = errors.New("planguard daily check is disabled")

// PlanguardRun is one corpus entry planned in one plan mode.
type PlanguardRun struct {
	Entry    string   `json:"entry"`
	Stmt     string   `json:"stmt"`
	Mode     string   `json:"mode"`               // custom or generic
	Verdict  string   `json:"verdict"`            // ok, DETECTED, MISSED, FALSE_POSITIVE, RETIRED_INDEX, KNOWN_ISSUE, SKIP or ERROR
	Failures []string `json:"failures,omitempty"` // the plan rules the run broke
	Drivers  []string `json:"drivers,omitempty"`  // relation:node:index of each driving scan
	Skip     string   `json:"skip,omitempty"`     // why the run was not made
}

// PlanguardReport is one read-only plan regression check of the corpus against this database.
type PlanguardReport struct {
	Tier       string         `json:"tier"`
	StartedAt  time.Time      `json:"started_at"`
	DurationMs float64        `json:"duration_ms"`
	Clean      bool           `json:"clean"`    // no FALSE_POSITIVE, MISSED or ERROR
	Counts     map[string]int `json:"counts"`   // verdict → runs
	Failures   []PlanguardRun `json:"failures"` // never null
	Runs       []PlanguardRun `json:"runs"`
}
