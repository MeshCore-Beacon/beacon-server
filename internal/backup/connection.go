// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package backup

import (
	"errors"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// ConnectionService captures a single-host PostgreSQL URL for pg_dump. The
// result contains secrets and must only be written to private staging, never
// logged or passed as a process argument. Unsupported settings fail closed.
func ConnectionService(dsn string) (string, error) {
	return connectionService(dsn, os.Environ())
}

func connectionService(dsn string, environ []string) (string, error) {
	invalid := errors.New("backup requires a single-host PostgreSQL URL with explicit user and database and supported libpq settings")
	u, err := url.Parse(dsn)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.User == nil || u.User.Username() == "" ||
		u.Hostname() == "" || strings.Contains(u.Hostname(), ",") || strings.TrimLeft(u.Path, "/") == "" || u.Fragment != "" {
		return "", invalid
	}
	settings := map[string]string{"host": u.Hostname(), "port": "5432", "user": u.User.Username(),
		"dbname": strings.TrimLeft(u.Path, "/"), "sslmode": "prefer", "gssencmode": "disable"}
	// Match the pgx startup environment for the supported native client settings.
	// Target fields come exclusively from the URL. Service indirection and newer
	// protocol options need separate compatibility work, not a guessed translation.
	envKeys := map[string]string{"PGPASSWORD": "password", "PGPASSFILE": "passfile", "PGAPPNAME": "application_name",
		"PGCONNECT_TIMEOUT": "connect_timeout", "PGSSLMODE": "sslmode", "PGSSLKEY": "sslkey", "PGSSLCERT": "sslcert",
		"PGSSLROOTCERT": "sslrootcert", "PGSSLPASSWORD": "sslpassword", "PGSSLSNI": "sslsni",
		"PGTARGETSESSIONATTRS": "target_session_attrs", "PGOPTIONS": "options"}
	for _, entry := range environ {
		key, value, _ := strings.Cut(entry, "=")
		if value == "" {
			continue
		}
		switch key {
		case "PGSERVICE", "PGSERVICEFILE", "PGSSLNEGOTIATION", "PGMINPROTOCOLVERSION", "PGMAXPROTOCOLVERSION", "PGTZ":
			return "", invalid
		}
		if name := envKeys[key]; name != "" {
			settings[name] = value
		}
		if key == "PGPORT" && u.Port() == "" {
			settings["port"] = value
		}
	}
	if port := u.Port(); port != "" {
		if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
			return "", invalid
		}
		settings["port"] = port
	}
	if password, ok := u.User.Password(); ok {
		settings["password"] = password
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return "", invalid
	}
	for key, values := range query {
		if len(values) != 1 || values[0] == "" {
			return "", invalid
		}
		switch key {
		case "pool_max_conns", "pool_min_conns", "pool_min_idle_conns", "pool_max_conn_lifetime", "pool_max_conn_idle_time", "pool_health_check_period", "pool_max_conn_lifetime_jitter":
			continue // pgx-only pool controls; main has already validated them.
		case "sslmode", "sslcert", "sslkey", "sslrootcert", "sslpassword", "sslsni", "passfile", "connect_timeout", "application_name", "target_session_attrs", "options":
			settings[key] = values[0]
		default:
			return "", invalid
		}
	}
	var result strings.Builder
	result.WriteString("[beacon_backup]\n")
	keys := make([]string, 0, len(settings))
	for key, value := range settings {
		// libpq service files have line-based, unquoted values. Reject anything
		// their parser would trim or interpret as another entry.
		if strings.TrimSpace(value) != value || strings.IndexFunc(value, unicode.IsControl) >= 0 || len(value) > 1000 {
			return "", invalid
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		result.WriteString(key + "=" + settings[key] + "\n")
	}
	return result.String(), nil
}
