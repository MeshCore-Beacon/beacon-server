// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package backup

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
)

var dumpVersion = regexp.MustCompile(`^pg_dump \(PostgreSQL\) ([0-9]+)(?:\.[0-9]+)+(?:[ \r\n]|$)`)

// CheckClient checks the client inside this runtime before enabling downloads.
// pg_dump cannot export a server newer than its own major version.
func CheckClient(ctx context.Context, serverVersion int) error {
	output, err := exec.CommandContext(ctx, "pg_dump", "--version").Output()
	if err != nil {
		return errors.New("cannot run pg_dump --version; install the PostgreSQL client in this runtime")
	}
	return checkClientVersion(output, serverVersion)
}

func checkClientVersion(output []byte, serverVersion int) error {
	match := dumpVersion.FindSubmatch(output)
	if match == nil || serverVersion < 100000 {
		return errors.New("cannot verify pg_dump and PostgreSQL server major versions")
	}
	client, err := strconv.Atoi(string(match[1]))
	if err != nil {
		return errors.New("cannot verify pg_dump major version")
	}
	if client < serverVersion/10000 {
		return fmt.Errorf("pg_dump major %d is older than PostgreSQL server major %d; install a compatible client and restart Beacon", client, serverVersion/10000)
	}
	return nil
}
