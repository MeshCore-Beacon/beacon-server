// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// beacon-backup creates or verifies a private database and saved-config export.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/MeshCore-Beacon/beacon-server/internal/backup"
)

var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	var opts backup.Options
	var verifyPath string
	flags := flag.NewFlagSet("beacon-backup", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&opts.ConfigPath, "config", "config.yaml", "saved YAML file to export verbatim (may contain secrets)")
	flags.StringVar(&opts.OutputPath, "output", "", "new private .tar.gz export destination (never overwritten)")
	flags.StringVar(&verifyPath, "verify", "", "verify an existing .tar.gz without extraction or database access")
	flags.Int64Var(&opts.MaxBytes, "max-bytes", backup.DefaultMaxBytes, "maximum uncompressed database dump size")
	flags.DurationVar(&opts.Timeout, "timeout", backup.DefaultTimeout, "maximum export or verification duration")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || opts.Timeout <= 0 {
		fmt.Fprintln(stderr, "positive timeout and no positional arguments required")
		return 2
	}
	verifyMode, exportFlag := false, false
	flags.Visit(func(f *flag.Flag) {
		verifyMode = verifyMode || f.Name == "verify"
		exportFlag = exportFlag || f.Name == "config" || f.Name == "output"
	})
	if verifyMode {
		if verifyPath == "" || exportFlag {
			fmt.Fprintln(stderr, "verify requires an archive path and cannot be combined with config or output")
			return 2
		}
		// Reject directories/devices/FIFOs before opening, and check the opened
		// descriptor too. A symlink to a regular, operator-selected file is fine.
		info, err := os.Stat(verifyPath)
		if err != nil || !info.Mode().IsRegular() {
			fmt.Fprintln(stderr, "verification requires a readable regular archive file")
			return 1
		}
		file, err := os.Open(verifyPath)
		if err != nil {
			fmt.Fprintln(stderr, "cannot open backup archive")
			return 1
		}
		defer file.Close()
		if info, err = file.Stat(); err != nil || !info.Mode().IsRegular() {
			fmt.Fprintln(stderr, "verification requires a regular archive file")
			return 1
		}
		ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
		if _, err := backup.Verify(ctx, file, opts.MaxBytes); err != nil {
			fmt.Fprintln(stderr, "verification failed:", err)
			return 1
		}
		fmt.Fprintln(stdout, "Archive structure and checksums verified. No files extracted or SQL executed.")
		return 0
	}
	opts.Version = version
	if err := backup.Export(ctx, opts); err != nil {
		fmt.Fprintln(stderr, "backup failed:", err)
		return 1
	}
	fmt.Fprintln(stdout, "Backup complete. Store this bundle privately; it may contain keys and message data.")
	return 0
}
