// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// beacon-backup creates a private database and saved-config export.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/MeshCore-Beacon/beacon-server/internal/backup"
)

var version = "dev"

func main() {
	var opts backup.Options
	flag.StringVar(&opts.ConfigPath, "config", "config.yaml", "saved YAML file to include verbatim (may contain secrets)")
	flag.StringVar(&opts.OutputPath, "output", "", "new private .tar.gz destination (required; never overwritten)")
	flag.Int64Var(&opts.MaxBytes, "max-bytes", backup.DefaultMaxBytes, "maximum uncompressed database dump size")
	flag.DurationVar(&opts.Timeout, "timeout", backup.DefaultTimeout, "maximum export duration")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments; connection settings use PG* environment variables")
		os.Exit(2)
	}
	opts.Version = version
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := backup.Export(ctx, opts); err != nil {
		fmt.Fprintln(os.Stderr, "backup failed:", err)
		os.Exit(1)
	}
	fmt.Println("Backup complete. Store this bundle privately; it may contain keys and message data.")
}
