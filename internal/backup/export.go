// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package backup exports a database and saved configuration into a private bundle.
package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

const (
	DefaultMaxBytes = int64(1 << 30)
	DefaultTimeout  = 10 * time.Minute
	maxConfigBytes  = 1 << 20
)

var ErrTooLarge = errors.New("backup size limit exceeded")

// Options selects local files and finite resource limits. Connection settings
// come from libpq's PG* environment; PGDATABASE must explicitly name the database.
type Options struct {
	ConfigPath string
	OutputPath string
	MaxBytes   int64 // Maximum uncompressed database dump size.
	Timeout    time.Duration
	Version    string
}

// Manifest describes format 1; hashes cover the uncompressed archive members.
type Manifest struct {
	FormatVersion  int       `json:"format_version"`
	CreatedAt      time.Time `json:"created_at"`
	ToolVersion    string    `json:"tool_version"`
	DatabaseFormat string    `json:"database_format"`
	Files          []File    `json:"files"`
	Excluded       []string  `json:"excluded"`
}

// File identifies one payload by its fixed archive name, size and SHA-256.
type File struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Export creates a complete .tar.gz without replacing an existing destination.
// pg_dump must be installed on PATH. The destination filesystem must support
// hard links; a link publishes the finished private file without an overwrite race.
// The bundle contains secrets if they are present in the database or saved YAML.
func Export(ctx context.Context, opts Options) error {
	return export(ctx, opts, dumpCommand)
}

func dumpCommand(ctx context.Context) *exec.Cmd {
	return exec.CommandContext(ctx, "pg_dump", "--format=plain", "--no-owner",
		"--no-acl", "--no-tablespaces", "--no-password", "--lock-wait-timeout=5000")
}

func export(ctx context.Context, opts Options, command func(context.Context) *exec.Cmd) error {
	if opts.ConfigPath == "" || opts.OutputPath == "" || opts.MaxBytes <= 0 || opts.MaxBytes > 1<<40 || opts.Timeout <= 0 {
		return errors.New("config, output, positive timeout and max-bytes (at most 1 TiB) are required")
	}
	if os.Getenv("PGDATABASE") == "" {
		return errors.New("PGDATABASE must explicitly name the database; POSTGRES_DSN is not used")
	}
	ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := os.Lstat(opts.OutputPath); !errors.Is(err, os.ErrNotExist) {
		return errors.New("output already exists or cannot be inspected")
	}
	config, err := readConfig(opts.ConfigPath)
	if err != nil {
		return err
	}
	parent, err := filepath.Abs(filepath.Dir(opts.OutputPath))
	if err != nil {
		return errors.New("invalid output directory")
	}
	temp, err := os.MkdirTemp(parent, ".beacon-backup-")
	if err != nil {
		return errors.New("cannot create private backup staging directory")
	}
	defer os.RemoveAll(temp)
	dump, err := os.OpenFile(filepath.Join(temp, "database.sql"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("cannot create private database dump")
	}
	defer dump.Close()
	hash := sha256.New()
	output := &limitedWriter{ctx: ctx, cancel: cancel, dst: io.MultiWriter(dump, hash), remaining: opts.MaxBytes}
	cmd := command(ctx)
	cmd.Stdout, cmd.Stderr = output, io.Discard
	cmd.WaitDelay = time.Second
	err = cmd.Run()
	if output.err != nil {
		return output.err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		// Client diagnostics may contain credentials, hostnames or database contents.
		return errors.New("pg_dump failed; check the installed client and private connection settings")
	}
	size := opts.MaxBytes - output.remaining
	if size == 0 {
		return errors.New("pg_dump produced an empty dump")
	}
	if _, err = dump.Seek(0, io.SeekStart); err != nil {
		return errors.New("cannot read completed database dump")
	}
	configHash := sha256.Sum256(config)
	manifest := Manifest{
		FormatVersion: 1, CreatedAt: time.Now().UTC(), ToolVersion: opts.Version,
		DatabaseFormat: "postgresql-plain-sql",
		Files: []File{
			{Name: "database.sql", Size: size, SHA256: hex.EncodeToString(hash.Sum(nil))},
			{Name: "config.yaml", Size: int64(len(config)), SHA256: hex.EncodeToString(configHash[:])},
		},
		Excluded: []string{"deployment_environment", "external_config_files", "runtime_only_changes", "roles_ownership_acl_tablespaces", "redis_cache"},
	}
	metadata, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return errors.New("cannot encode backup manifest")
	}
	archivePath := filepath.Join(temp, "bundle.tar.gz")
	archive, err := os.OpenFile(archivePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("cannot create private backup archive")
	}
	defer archive.Close()
	// Input is bounded; reserve room for YAML, tar headers and compression overhead.
	archiveOutput := &limitedWriter{ctx: ctx, cancel: cancel, dst: archive, remaining: opts.MaxBytes + opts.MaxBytes/100 + 2*maxConfigBytes}
	gz := gzip.NewWriter(archiveOutput)
	tw := tar.NewWriter(gz)
	for _, member := range []struct {
		name string
		size int64
		data io.Reader
	}{
		{"manifest.json", int64(len(metadata)), bytes.NewReader(metadata)},
		{"database.sql", size, dump},
		{"config.yaml", int64(len(config)), bytes.NewReader(config)},
	} {
		if err = tw.WriteHeader(&tar.Header{Name: member.name, Size: member.size, Mode: 0600, ModTime: manifest.CreatedAt}); err == nil {
			_, err = io.Copy(tw, member.data)
		}
		if err != nil {
			return errors.New("cannot write complete backup archive")
		}
	}
	if err = tw.Close(); err != nil {
		return errors.New("cannot complete backup tar")
	}
	if err = gz.Close(); err != nil {
		return errors.New("cannot complete backup compression")
	}
	if err = archive.Sync(); err != nil {
		return errors.New("cannot sync completed backup")
	}
	if err = archive.Close(); err != nil {
		return errors.New("cannot close completed backup")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = os.Link(archivePath, opts.OutputPath); err != nil {
		return errors.New("cannot publish backup without overwriting; check destination and hard-link support")
	}
	return nil
}

func readConfig(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxConfigBytes {
		return nil, errors.New("config must be a readable regular file of at most 1 MiB")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot read saved config")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxConfigBytes+1))
	if err != nil || len(data) > maxConfigBytes {
		return nil, errors.New("cannot read saved config within the 1 MiB limit")
	}
	return data, nil
}

type limitedWriter struct {
	ctx       context.Context
	cancel    context.CancelFunc
	dst       io.Writer
	remaining int64
	err       error
}

func (w *limitedWriter) Write(p []byte) (n int, err error) {
	if w.err == nil {
		w.err = w.ctx.Err()
	}
	if w.err == nil && int64(len(p)) > w.remaining {
		w.err = ErrTooLarge
	}
	if w.err == nil {
		n, w.err = w.dst.Write(p)
		w.remaining -= int64(n)
		if w.err == nil && n != len(p) {
			w.err = io.ErrShortWrite
		}
	}
	if w.err != nil {
		w.cancel()
	}
	return n, w.err
}
