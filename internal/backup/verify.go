// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package backup

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const maxManifestBytes = 64 << 10

var ErrInvalidArchive = errors.New("invalid backup archive")

// Verify streams a format-1 bundle without extracting files or executing SQL.
// maxBytes bounds the SQL payload (up to 1 TiB); the saved YAML and manifest have
// separate fixed limits. The caller owns input and its deadline. Cancellation is
// checked between reads; the supplied Reader must not block indefinitely.
// The returned manifest is untrusted metadata, not proof of origin or restorability.
func Verify(ctx context.Context, input io.Reader, maxBytes int64) (*Manifest, error) {
	if input == nil || maxBytes <= 0 || maxBytes > 1<<40 {
		return nil, errors.New("input and positive max-bytes (at most 1 TiB) are required")
	}
	// Match the exporter's compressed-output allowance. ByteReader prevents gzip
	// from swallowing a following stream before we can reject it.
	compressed := &verifyReader{ctx: ctx, r: input, limit: maxBytes + maxBytes/100 + 2*maxConfigBytes}
	buffered := bufio.NewReader(compressed)
	gz, err := gzip.NewReader(buffered)
	if err != nil {
		return nil, verificationError(ctx, err, "gzip header")
	}
	defer gz.Close()
	gz.Multistream(false)
	decoded := &verifyReader{ctx: ctx, r: gz}
	tr := tar.NewReader(decoded)
	files := make(map[string]File, 3)
	var metadata []byte
	var padding int64
	for {
		before := decoded.n
		// Native headers are USTAR, or one size-only PAX record for SQL >8 GiB.
		// Bound Next itself: it otherwise hides and allocates extension records.
		decoded.limit = before + padding + 3*512
		h, err := tr.Next()
		if err == io.EOF {
			// tar.Reader also accepts EOF without both closing zero blocks.
			if decoded.n-before != padding+2*512 {
				return nil, verificationError(ctx, nil, "missing tar terminator")
			}
			break
		}
		if err != nil {
			return nil, verificationError(ctx, err, "tar header")
		}
		headerBytes := decoded.n - before - padding
		ordinary := h.Format == tar.FormatUSTAR && headerBytes == 512 && len(h.PAXRecords) == 0
		large := h.Format == tar.FormatPAX && headerBytes == 3*512 && len(h.PAXRecords) == 1 && h.PAXRecords["size"] == strconv.FormatInt(h.Size, 10)
		if (!ordinary && !large) || (h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeRegA) || h.Linkname != "" || h.Mode != 0600 {
			return nil, verificationError(ctx, nil, "unsupported member type or metadata")
		}
		if _, duplicate := files[h.Name]; duplicate || len(files) == 3 {
			return nil, verificationError(ctx, nil, "duplicate or extra member")
		}
		var limit int64
		switch h.Name {
		case "manifest.json":
			limit = maxManifestBytes
		case "database.sql":
			limit = maxBytes
			if h.Size == 0 {
				return nil, verificationError(ctx, nil, "empty database dump")
			}
		case "config.yaml":
			limit = maxConfigBytes
		default:
			return nil, verificationError(ctx, nil, "unexpected member name")
		}
		if h.Size < 0 || h.Size > limit {
			return nil, ErrTooLarge
		}
		decoded.limit = decoded.n + h.Size
		digest := sha256.New()
		if h.Name == "manifest.json" {
			metadata, err = io.ReadAll(tr)
		} else {
			_, err = io.Copy(digest, tr)
		}
		if err != nil {
			return nil, verificationError(ctx, err, "member data")
		}
		files[h.Name] = File{Name: h.Name, Size: h.Size, SHA256: hex.EncodeToString(digest.Sum(nil))}
		padding = (512 - h.Size%512) % 512
	}
	// Read through the gzip checksum/trailer, but allow no data after tar EOF.
	decoded.limit = decoded.n
	var tail [1]byte
	if _, err := decoded.Read(tail[:]); err != io.EOF {
		return nil, verificationError(ctx, err, "gzip completion or trailing tar data")
	}
	if _, err := buffered.ReadByte(); err != io.EOF {
		return nil, verificationError(ctx, err, "trailing compressed data")
	}
	if len(files) != 3 || uniqueManifestKeys(json.NewDecoder(bytes.NewReader(metadata)), 0) != nil {
		return nil, verificationError(ctx, nil, "missing members or invalid manifest")
	}
	var manifest Manifest
	decoder := json.NewDecoder(bytes.NewReader(metadata))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil || decoder.Decode(new(any)) != io.EOF || manifest.FormatVersion != 1 || manifest.DatabaseFormat != "postgresql-plain-sql" || manifest.CreatedAt.IsZero() || len(manifest.Files) != 2 {
		return nil, verificationError(ctx, nil, "unsupported or incomplete manifest")
	}
	for _, declared := range manifest.Files {
		actual, exists := files[declared.Name]
		digest, err := hex.DecodeString(declared.SHA256)
		if !exists || declared.Name == "manifest.json" || declared.Size != actual.Size || err != nil || len(digest) != sha256.Size || !strings.EqualFold(declared.SHA256, actual.SHA256) {
			return nil, verificationError(ctx, nil, "manifest size or checksum mismatch")
		}
		delete(files, declared.Name) // duplicate manifest file records fail as well.
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &manifest, nil
}

// Avoid returning filenames, manifest values, reader errors or payload text.
func verificationError(ctx context.Context, err error, stage string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, ErrTooLarge) {
		return ErrTooLarge
	}
	return fmt.Errorf("%w: %s", ErrInvalidArchive, stage)
}

type verifyReader struct {
	ctx      context.Context
	r        io.Reader
	n, limit int64
}

func (r *verifyReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.n > r.limit {
		return 0, ErrTooLarge
	}
	if int64(len(p)) > r.limit-r.n+1 {
		p = p[:r.limit-r.n+1]
	}
	n, err := r.r.Read(p)
	r.n += int64(n)
	if r.n > r.limit {
		return 0, ErrTooLarge
	}
	return n, err
}

// encoding/json accepts duplicate keys and case aliases. Reject both before
// decoding the fixed schema, so another reader cannot interpret different values.
func uniqueManifestKeys(d *json.Decoder, depth int) error {
	if depth > 8 {
		return ErrInvalidArchive
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); ok {
		keys := map[string]bool{}
		for d.More() {
			if delim == '{' {
				key, err := d.Token()
				if err != nil {
					return err
				}
				name, ok := key.(string)
				// Schema keys are ASCII. Unicode simple-fold aliases (such as
				// long-s for s) also match encoding/json fields, even in lowercase.
				if !ok || keys[name] || strings.IndexFunc(name, func(r rune) bool { return r > 127 || r >= 'A' && r <= 'Z' }) >= 0 {
					return ErrInvalidArchive
				}
				keys[name] = true
			}
			if err := uniqueManifestKeys(d, depth+1); err != nil {
				return err
			}
		}
		_, err = d.Token()
	}
	return err
}
