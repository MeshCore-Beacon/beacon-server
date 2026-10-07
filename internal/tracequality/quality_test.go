package tracequality

import (
	"bytes"
	"slices"
	"testing"
)

func TestTraceFormat(t *testing.T) {
	for flags := byte(0); flags < 4; flags++ {
		raw := make([]byte, 9)
		raw[8] = flags
		raw = append(raw, bytes.Repeat([]byte{0xaa}, 3*(1<<flags))...)
		for _, consumed := range []int{0, 1, 3} {
			if q := Assess(raw, 0, 2, consumed); q.Status != "supported" {
				t.Fatalf("valid width %d, partial/loop trace: %+v", 1<<flags, q)
			}
		}
	}
	cases := []struct {
		name                               string
		version, route, path, flags, count int
		reason                             string
	}{
		{"version", 1, 2, 0, 0, 3, "unsupported_version"},
		{"flood", 0, 1, 0, 0, 3, "non_direct_trace"},
		{"flags", 0, 2, 0, 133, 74, "unsupported_flags"},
		{"101 hops", 0, 2, 55, 0, 101, "too_many_hops"},
		{"145 hops", 0, 2, 10, 0, 145, "too_many_hops"},
		{"ordinary multibyte metadata is not trace SNR count", 0, 2, 65, 1, 3, "invalid_snr_path"},
		{"extra SNR", 0, 2, 4, 0, 3, "invalid_snr_path"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := make([]byte, 9)
			raw[8] = byte(tc.flags)
			raw = append(raw, bytes.Repeat([]byte{0xaa}, tc.count*(1<<(tc.flags&3)))...)
			q := Assess(raw, tc.version, tc.route, tc.path)
			if q.Status != "suspect" || !slices.Contains(q.Reasons, tc.reason) {
				t.Fatalf("%+v", q)
			}
		})
	}
	if q := Assess([]byte{0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 2}, 0, 2, 0); !slices.Contains(q.Reasons, "misaligned_hashes") {
		t.Fatal(q)
	}
	// Legitimate missing reports, negative SNR, and a returning loop are not corruption.
	if q := Stored("", 0, []string{"aa", "bb", "aa"}, []float32{-12.25, 0}); q.Status != "supported" {
		t.Fatal(q)
	}
}
