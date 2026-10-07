// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package tracequality checks TRACE structure, not authenticity or RF plausibility.
package tracequality

import "encoding/hex"

type Quality struct {
	Status  string   `json:"status"` // supported, suspect, or ambiguous; supported is not authenticated
	Reasons []string `json:"reasons"`
}

func (q *Quality) Add(reason string) {
	for _, r := range q.Reasons {
		if r == reason {
			return
		}
	}
	q.Reasons = append(q.Reasons, reason)
	if reason == "ambiguous_prefix" && q.Status != "suspect" {
		q.Status = "ambiguous"
	} else if reason != "ambiguous_prefix" {
		q.Status = "suspect"
	}
}

// Assess uses TRACE's power-of-two payload hash widths, NOT the ordinary
// packet path's 1/2/3-byte widths. The path field contains consumed-hop SNRs.
// -1 means a header/path field is unavailable in a retained summary.
// Mesh.cpp onRecvPacket/sendDirect: a trace uses direct routing, flags bits
// 0..1, and fewer than 64 SNR bytes. Partial probes and return loops are valid.
func Assess(raw []byte, version, route, pathLength int) Quality {
	q := Quality{Status: "supported", Reasons: []string{}}
	if version > 0 {
		q.Add("unsupported_version")
	}
	if route >= 0 && route != 2 && route != 3 {
		q.Add("non_direct_trace")
	}
	if len(raw) < 10 {
		q.Add("incomplete_payload")
		return q
	}
	if len(raw) > 184 {
		q.Add("payload_too_large")
	}
	if raw[8]&0xfc != 0 {
		q.Add("unsupported_flags")
	}
	size := 1 << (raw[8] & 3)
	if (len(raw)-9)%size != 0 {
		q.Add("misaligned_hashes")
	}
	count := (len(raw) - 9) / size
	if count > 63 {
		q.Add("too_many_hops")
	}
	if pathLength > 63 || pathLength > count {
		q.Add("invalid_snr_path")
	}
	return q
}

// Stored also checks old parsed rows, without rewriting or deleting the evidence.
func Stored(raw string, flags byte, hashes []string, snrs []float32) Quality {
	b, err := hex.DecodeString(raw)
	if err != nil {
		return Quality{Status: "suspect", Reasons: []string{"incomplete_payload"}}
	}
	if raw == "" { // Older exports may contain only decoded fields.
		b = make([]byte, 9)
		b[8] = flags
		for _, h := range hashes {
			v, e := hex.DecodeString(h)
			if e != nil {
				return Quality{Status: "suspect", Reasons: []string{"misaligned_hashes"}}
			}
			b = append(b, v...)
		}
	}
	q := Assess(b, -1, -1, len(snrs))
	size := 1 << (flags & 3)
	if len(b) >= 9 && b[8] != flags {
		q.Add("misaligned_hashes")
	}
	for _, h := range hashes {
		if len(h) != 2*size {
			q.Add("misaligned_hashes")
		}
	}
	if len(b) >= 9 && len(hashes) != (len(b)-9)/size {
		q.Add("misaligned_hashes")
	}
	return q
}
