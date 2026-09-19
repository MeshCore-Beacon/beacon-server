// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package backup

import (
	"strings"
	"testing"
)

func TestConnectionService(t *testing.T) {
	service, err := connectionService("postgresql://user:p%23ass%3Dword@[::1]:5544/db%20name?sslmode=verify-full&sslrootcert=%2Fprivate%2Fca.pem&passfile=%2Fprivate%2Fpass&pool_max_conns=2", []string{
		"PGHOST=wrong", "PGDATABASE=wrong", "PGUSER=wrong", "PGPASSWORD=wrong", "PGSSLMODE=disable", "PGPASSFILE=/wrong", "PGSSLKEY=/private/key.pem"})
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{"[beacon_backup]", "host=::1", "port=5544", "user=user", "password=p#ass=word", "dbname=db name",
		"sslmode=verify-full", "sslrootcert=/private/ca.pem", "passfile=/private/pass", "sslkey=/private/key.pem"} {
		if !strings.Contains(service, line+"\n") {
			t.Fatalf("missing expected setting %q", line)
		}
	}
	if strings.Contains(service, "wrong") || strings.Contains(service, "pool_") {
		t.Fatal("unrelated target or pool settings leaked into libpq")
	}
	service, err = connectionService("postgres://user@host/db", []string{"PGPASSWORD=from-environment", "PGPORT=5555", "PGSSLMODE=require"})
	if err != nil || !strings.Contains(service, "password=from-environment\n") || !strings.Contains(service, "port=5555\n") || !strings.Contains(service, "sslmode=require\n") {
		t.Fatal("supported environment defaults were not captured")
	}
}

func TestConnectionServiceRejectsAmbiguousSettings(t *testing.T) {
	for _, dsn := range []string{
		"host=host user=user dbname=db", "postgres://host/db", "postgres://user@host/", "postgres://user@a,b/db",
		"postgres://user@host/db?service=other", "postgres://user@host/db?host=other", "postgres://user@host/db?sslmode=require&sslmode=disable",
		"postgres://user@host/db?unknown=x", "postgres://user:secret%0Ahost%3Dother@host/db", "postgres://user:%20secret@host/db",
		"postgres://user@host:99999/db", "postgres://user@host/db#fragment", "postgres://user@host/db?sslnegotiation=direct",
	} {
		if _, err := connectionService(dsn, nil); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatal("unsupported connection must fail without echoing input")
		}
	}
	for _, env := range []string{"PGSERVICE=other", "PGSERVICEFILE=/private", "PGSSLNEGOTIATION=direct", "PGMINPROTOCOLVERSION=3.0", "PGTZ=UTC"} {
		if _, err := connectionService("postgres://user@host/db", []string{env}); err == nil {
			t.Fatal("unsupported ambient connection settings accepted")
		}
	}
}
