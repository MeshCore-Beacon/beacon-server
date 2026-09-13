// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

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
	"runtime"
	"strings"
	"testing"
	"time"
)

const testSQL = "CREATE TABLE fixture (id bigint PRIMARY KEY, name text);\nINSERT INTO fixture VALUES (1, 'café');\n"

// TestDumpProcess supplies an actual subprocess, including failure and cancellation.
func TestDumpProcess(t *testing.T) {
	mode := os.Getenv("BEACON_BACKUP_TEST_PROCESS")
	if mode == "" {
		return
	}
	switch mode {
	case "ok":
		_, _ = io.WriteString(os.Stdout, testSQL)
	case "fail":
		_, _ = io.WriteString(os.Stdout, "incomplete dump")
		_, _ = io.WriteString(os.Stderr, "PRIVATE_PASSWORD_CANARY")
		os.Exit(17)
	case "large":
		_, _ = os.Stdout.Write(bytes.Repeat([]byte("x"), 65536))
	case "hang":
		_, _ = io.WriteString(os.Stdout, "incomplete dump")
		time.Sleep(time.Minute)
	case "empty":
	default:
		os.Exit(2)
	}
	os.Exit(0)
}

func helper(t *testing.T, mode string) func(context.Context) *exec.Cmd {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return func(ctx context.Context) *exec.Cmd {
		cmd := exec.CommandContext(ctx, exe, "-test.run=^TestDumpProcess$")
		cmd.Env = append(os.Environ(), "BEACON_BACKUP_TEST_PROCESS="+mode)
		return cmd
	}
}

func setup(t *testing.T) Options {
	t.Helper()
	t.Setenv("PGDATABASE", "fixture")
	dir := t.TempDir()
	opts := Options{ConfigPath: filepath.Join(dir, "saved.yaml"), OutputPath: filepath.Join(dir, "backup.tar.gz"),
		MaxBytes: int64(len(testSQL)), Timeout: 10 * time.Second, Version: "test-revision"}
	if err := os.WriteFile(opts.ConfigPath, []byte("# preserve comments and this synthetic key\nchannel_keys:\n  keys:\n    '00': {key: '00000000000000000000000000000000', name: fixture}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		paths, err := filepath.Glob(filepath.Join(dir, ".beacon-backup-*"))
		if err != nil || len(paths) != 0 {
			t.Errorf("private staging was not cleaned: %v %v", paths, err)
		}
	})
	return opts
}

func TestExportBundle(t *testing.T) {
	opts := setup(t)
	if err := export(context.Background(), opts, helper(t, "ok")); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(opts.OutputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || (runtime.GOOS != "windows" && info.Mode().Perm() != 0600) {
		t.Fatalf("backup must be private: %v %v", info, err)
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	files := map[string][]byte{}
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil || header.Mode != 0600 || header.Typeflag != tar.TypeReg {
			t.Fatalf("invalid archive member: %v %v", header, err)
		}
		if _, duplicate := files[header.Name]; duplicate {
			t.Fatal("duplicate archive name")
		}
		files[header.Name], err = io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := io.Copy(io.Discard, gz); err != nil {
		t.Fatal("invalid gzip footer:", err)
	}
	config, _ := os.ReadFile(opts.ConfigPath)
	if len(files) != 3 || string(files["database.sql"]) != testSQL || !bytes.Equal(files["config.yaml"], config) {
		t.Fatal("payloads differ from the supplied SQL and saved config")
	}
	var manifest Manifest
	if err := json.Unmarshal(files["manifest.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.FormatVersion != 1 || manifest.ToolVersion != opts.Version || manifest.CreatedAt.IsZero() || len(manifest.Files) != 2 || len(manifest.Excluded) != 5 {
		t.Fatalf("incomplete manifest: %+v", manifest)
	}
	for _, member := range manifest.Files {
		digest := sha256.Sum256(files[member.Name])
		if member.Size != int64(len(files[member.Name])) || member.SHA256 != hex.EncodeToString(digest[:]) {
			t.Errorf("manifest mismatch for %s", member.Name)
		}
	}
}

func TestExportFailureCleanup(t *testing.T) {
	for _, mode := range []string{"fail", "large", "hang", "empty"} {
		t.Run(mode, func(t *testing.T) {
			opts := setup(t)
			if mode == "hang" {
				opts.Timeout = time.Second
			}
			err := export(context.Background(), opts, helper(t, mode))
			if err == nil || strings.Contains(err.Error(), "PRIVATE_PASSWORD_CANARY") {
				t.Fatalf("expected sanitized failure, got %v", err)
			}
			if mode == "large" && !errors.Is(err, ErrTooLarge) {
				t.Fatalf("wrong limit error: %v", err)
			}
			if mode == "hang" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("wrong timeout error: %v", err)
			}
			if _, err := os.Lstat(opts.OutputPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("failed export published an output")
			}
		})
	}
}

func TestExportNeverReplacesDestination(t *testing.T) {
	for _, race := range []bool{false, true} {
		t.Run(map[bool]string{false: "existing", true: "created-during-export"}[race], func(t *testing.T) {
			opts := setup(t)
			putExisting := func() {
				if err := os.WriteFile(opts.OutputPath, []byte("retain this backup"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if !race {
				putExisting()
			}
			cmd := helper(t, "ok")
			err := export(context.Background(), opts, func(ctx context.Context) *exec.Cmd {
				if !race {
					t.Fatal("existing output must be rejected before starting pg_dump")
				}
				putExisting()
				return cmd(ctx)
			})
			data, readErr := os.ReadFile(opts.OutputPath)
			if err == nil || readErr != nil || string(data) != "retain this backup" {
				t.Fatalf("existing backup changed: %v %v", err, readErr)
			}
		})
	}
}

func TestExportValidation(t *testing.T) {
	for _, mode := range []string{"database", "limit", "config", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			opts := setup(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "database":
				t.Setenv("PGDATABASE", "")
			case "limit":
				opts.MaxBytes = 0
			case "config":
				if err := os.WriteFile(opts.ConfigPath, make([]byte, maxConfigBytes+1), 0600); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				cancel()
			}
			if err := export(ctx, opts, func(context.Context) *exec.Cmd { t.Fatal("dump ran on invalid input"); return nil }); err == nil {
				t.Fatal("invalid export succeeded")
			}
		})
	}
}

func TestDumpArgumentsExcludeConnectionSecrets(t *testing.T) {
	t.Setenv("PGPASSWORD", "PRIVATE_PASSWORD_CANARY")
	t.Setenv("POSTGRES_DSN", "postgres://user:PRIVATE_PASSWORD_CANARY@localhost/db")
	cmd := dumpCommand(context.Background())
	if strings.Contains(strings.Join(cmd.Args, " "), "PRIVATE_PASSWORD_CANARY") {
		t.Fatal("credentials reached command arguments")
	}
	args := strings.Join(cmd.Args, " ")
	for _, required := range []string{"--no-password", "--lock-wait-timeout=5000", "--no-owner", "--no-acl", "--no-tablespaces", "--format=plain"} {
		if !strings.Contains(args, required) {
			t.Errorf("missing dump boundary: %s", required)
		}
	}
}
