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
	"strings"
	"testing"
	"time"
)

type verifyEntry struct {
	name string
	data []byte
	kind byte
	pax  map[string]string
}

func verifyFixture(t testing.TB) []verifyEntry {
	t.Helper()
	sql, config := []byte(testSQL), []byte("secret: PRIVATE_CANARY\n")
	m := Manifest{FormatVersion: 1, CreatedAt: time.Now().UTC(), ToolVersion: "fixture", DatabaseFormat: "postgresql-plain-sql", Excluded: []string{"deployment_environment"}}
	for _, entry := range []verifyEntry{{name: "database.sql", data: sql}, {name: "config.yaml", data: config}} {
		hash := sha256.Sum256(entry.data)
		m.Files = append(m.Files, File{Name: entry.name, Size: int64(len(entry.data)), SHA256: hex.EncodeToString(hash[:])})
	}
	metadata, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return []verifyEntry{{name: "manifest.json", data: metadata}, {name: "database.sql", data: sql}, {name: "config.yaml", data: config}}
}

func verifyTar(t testing.TB, entries []verifyEntry) []byte {
	t.Helper()
	var data bytes.Buffer
	tw := tar.NewWriter(&data)
	for _, e := range entries {
		h := &tar.Header{Name: e.name, Size: int64(len(e.data)), Mode: 0600, Typeflag: e.kind}
		if e.kind == tar.TypeSymlink || e.kind == tar.TypeLink {
			h.Linkname = "../PRIVATE_CANARY"
		}
		if e.pax != nil {
			h.Format, h.PAXRecords = tar.FormatPAX, e.pax
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(e.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return data.Bytes()
}

func verifyGzip(t testing.TB, data []byte) []byte {
	t.Helper()
	var encoded bytes.Buffer
	gz := gzip.NewWriter(&encoded)
	if _, err := gz.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func TestVerifyExportAndReorderedMembers(t *testing.T) {
	opts := setup(t)
	if err := export(context.Background(), opts, helper(t, "ok")); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(opts.OutputPath)
	if err != nil {
		t.Fatal(err)
	}
	m, err := Verify(context.Background(), bytes.NewReader(original), opts.MaxBytes)
	if err != nil || m.ToolVersion != opts.Version || len(m.Files) != 2 {
		t.Fatalf("native export: %v %v", m, err)
	}
	entries := verifyFixture(t)
	entries[0], entries[2] = entries[2], entries[0]
	if _, err := Verify(context.Background(), bytes.NewReader(verifyGzip(t, verifyTar(t, entries))), int64(len(testSQL))); err != nil {
		t.Fatal("valid reordered members:", err)
	}
	if current, err := os.ReadFile(opts.OutputPath); err != nil || !bytes.Equal(original, current) {
		t.Fatal("verification changed input")
	}
}

func TestVerifyRejectsInvalidMembersAndManifest(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func([]verifyEntry) []verifyEntry
	}{
		{"missing", func(e []verifyEntry) []verifyEntry { return e[:2] }},
		{"duplicate", func(e []verifyEntry) []verifyEntry { return append(e, e[1]) }},
		{"traversal", func(e []verifyEntry) []verifyEntry { e[2].name = "../PRIVATE_CANARY"; return e }},
		{"absolute", func(e []verifyEntry) []verifyEntry { e[2].name = "/config.yaml"; return e }},
		{"windows path", func(e []verifyEntry) []verifyEntry { e[2].name = `C:\config.yaml`; return e }},
		{"directory prefix", func(e []verifyEntry) []verifyEntry { e[2].name = "./config.yaml"; return e }},
		{"symlink", func(e []verifyEntry) []verifyEntry { e[2].kind, e[2].data = tar.TypeSymlink, nil; return e }},
		{"hardlink", func(e []verifyEntry) []verifyEntry { e[2].kind, e[2].data = tar.TypeLink, nil; return e }},
		{"device", func(e []verifyEntry) []verifyEntry { e[2].kind, e[2].data = tar.TypeChar, nil; return e }},
		{"directory", func(e []verifyEntry) []verifyEntry { e[2].kind, e[2].data = tar.TypeDir, nil; return e }},
		{"payload tamper", func(e []verifyEntry) []verifyEntry { e[1].data[0] ^= 1; return e }},
		{"large sql", func(e []verifyEntry) []verifyEntry { e[1].data = bytes.Repeat([]byte("x"), len(testSQL)+1); return e }},
		{"large config", func(e []verifyEntry) []verifyEntry { e[2].data = bytes.Repeat([]byte("x"), maxConfigBytes+1); return e }},
		{"large manifest", func(e []verifyEntry) []verifyEntry {
			e[0].data = bytes.Repeat([]byte("x"), maxManifestBytes+1)
			return e
		}},
		{"extended attributes", func(e []verifyEntry) []verifyEntry {
			e[1].pax = map[string]string{"SCHILY.xattr.secret": "PRIVATE_CANARY"}
			return e
		}},
		{"hidden oversized metadata", func(e []verifyEntry) []verifyEntry {
			e[1].pax = map[string]string{"comment": strings.Repeat("x", 8000)}
			return e
		}},
		{"unknown version", func(e []verifyEntry) []verifyEntry {
			e[0].data = bytes.Replace(e[0].data, []byte(`"format_version":1`), []byte(`"format_version":2`), 1)
			return e
		}},
		{"duplicate key", func(e []verifyEntry) []verifyEntry {
			e[0].data = append([]byte(`{"format_version":2,`), e[0].data[1:]...)
			return e
		}},
		{"case alias", func(e []verifyEntry) []verifyEntry {
			e[0].data = bytes.Replace(e[0].data, []byte("format_version"), []byte("FORMAT_VERSION"), 1)
			return e
		}},
		{"unicode case alias", func(e []verifyEntry) []verifyEntry {
			e[0].data = bytes.Replace(e[0].data, []byte("database_format"), []byte(`databa\u017fe_format`), 1)
			return e
		}},
		{"duplicate file record", func(e []verifyEntry) []verifyEntry {
			var m Manifest
			_ = json.Unmarshal(e[0].data, &m)
			m.Files[1] = m.Files[0]
			e[0].data, _ = json.Marshal(m)
			return e
		}},
		{"claimed size", func(e []verifyEntry) []verifyEntry {
			var m Manifest
			_ = json.Unmarshal(e[0].data, &m)
			m.Files[0].Size++
			e[0].data, _ = json.Marshal(m)
			return e
		}},
		{"unknown field", func(e []verifyEntry) []verifyEntry {
			e[0].data = append([]byte(`{"private":"PRIVATE_CANARY",`), e[0].data[1:]...)
			return e
		}},
		{"second json value", func(e []verifyEntry) []verifyEntry { e[0].data = append(e[0].data, []byte(`{}`)...); return e }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := verifyGzip(t, verifyTar(t, tc.change(verifyFixture(t))))
			if _, err := Verify(context.Background(), bytes.NewReader(data), int64(len(testSQL))); err == nil || strings.Contains(err.Error(), "PRIVATE_CANARY") {
				t.Fatalf("expected a sanitized rejection, got %v", err)
			}
		})
	}
}

func TestVerifyFraming(t *testing.T) {
	raw := verifyTar(t, verifyFixture(t))
	valid := verifyGzip(t, raw)
	badCRC := bytes.Clone(valid)
	badCRC[len(badCRC)-8] ^= 1
	cases := map[string][]byte{
		"not gzip": []byte("PRIVATE_CANARY"), "truncated gzip": valid[:len(valid)-1], "bad checksum": badCRC,
		"no tar terminator": verifyGzip(t, raw[:len(raw)-1024]), "one tar terminator": verifyGzip(t, raw[:len(raw)-512]),
		"truncated payload": verifyGzip(t, raw[:1500]), "trailing tar bytes": verifyGzip(t, append(bytes.Clone(raw), 0)),
		"trailing compressed bytes": append(bytes.Clone(valid), 0), "second gzip stream": append(bytes.Clone(valid), verifyGzip(t, nil)...),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Verify(context.Background(), bytes.NewReader(data), DefaultMaxBytes); err == nil || strings.Contains(err.Error(), "PRIVATE_CANARY") {
				t.Fatalf("expected sanitized framing error, got %v", err)
			}
		})
	}
}

func TestVerifySizeOnlyPAX(t *testing.T) {
	// A native dump over 8 GiB requires a size-only PAX header. Generate the
	// real header without allocating its body; verification must reach the
	// truncated member, rather than reject the exporter's metadata format.
	var raw bytes.Buffer
	tw := tar.NewWriter(&raw)
	if err := tw.WriteHeader(&tar.Header{Name: "database.sql", Mode: 0600, Size: 1 << 34}); err != nil {
		t.Fatal(err)
	}
	h, err := tar.NewReader(bytes.NewReader(raw.Bytes())).Next()
	if err != nil || h.Format != tar.FormatPAX || len(h.PAXRecords) != 1 || h.PAXRecords["size"] != "17179869184" {
		t.Fatalf("fixture must contain native size-only PAX metadata: %v %v", h, err)
	}
	data := verifyGzip(t, raw.Bytes())
	if _, err := Verify(context.Background(), bytes.NewReader(data), 1<<35); !errors.Is(err, ErrInvalidArchive) || !strings.Contains(err.Error(), "member data") {
		t.Fatalf("native PAX header was rejected before reading the payload: %v", err)
	}
	if _, err := Verify(context.Background(), bytes.NewReader(data), DefaultMaxBytes); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("PAX size did not respect the SQL limit: %v", err)
	}
}

func TestVerifyBoundsAndCancellation(t *testing.T) {
	for _, limit := range []int64{0, -1, 1<<40 + 1} {
		if _, err := Verify(context.Background(), bytes.NewReader(nil), limit); err == nil {
			t.Fatal("invalid limit accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Verify(ctx, bytes.NewReader(nil), 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	for _, size := range []int{2, 3} {
		r := &verifyReader{ctx: context.Background(), r: bytes.NewReader(make([]byte, size)), limit: 2}
		_, err := io.Copy(io.Discard, r)
		if (size == 3 && !errors.Is(err, ErrTooLarge)) || (size == 2 && err != nil) {
			t.Fatalf("read limit boundary: %d %v", size, err)
		}
	}
}

func FuzzVerify(f *testing.F) {
	f.Add([]byte("invalid gzip"))
	f.Add(verifyGzip(f, verifyTar(f, verifyFixture(f))))
	f.Add(verifyTar(f, verifyFixture(f)))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 2<<20 {
			t.Skip()
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, _ = Verify(ctx, bytes.NewReader(data), 4096)
		// Fuzz tar/manifest parsing too, without every mutation first failing
		// the outer gzip checksum.
		_, _ = Verify(ctx, bytes.NewReader(verifyGzip(t, data)), 4096)
	})
}
