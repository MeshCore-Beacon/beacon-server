// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

type vector struct {
	Name   string `json:"name"`
	ObsPrv string `json:"obsPrv"`
	ObsPub string `json:"obsPub"`
	Target string `json:"target"`
	Epk    string `json:"epk"`
	Secret string `json:"secret"`
	Ct     string `json:"ct"`
	Tag    string `json:"tag"`
	Pw     string `json:"pw"`
	Ver    uint32 `json:"ver"`
}

func loadVectors(t *testing.T) (good, bad []vector) {
	t.Helper()
	raw, err := os.ReadFile("testdata/pw_vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var f struct{ Good, Bad []vector }
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Good) != 3 || len(f.Bad) != 3 {
		t.Fatalf("vectors: %d good %d bad", len(f.Good), len(f.Bad))
	}
	return f.Good, f.Bad
}

func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// ephSeed is the one-time seed gen_vectors.py uses for vector n.
func ephSeed(n int) []byte {
	b := make([]byte, 32)
	for i := range b {
		b[i] = byte(n*5 + 9 + i*13)
	}
	return b
}

func TestSealReproducesFirmwareVectors(t *testing.T) {
	good, _ := loadVectors(t)
	for i, v := range good {
		env, err := SealEnvelope(bytes.NewReader(ephSeed(i+1)), unhex(t, v.ObsPub), unhex(t, v.Target), v.Ver, unhex(t, v.Pw))
		if err != nil {
			t.Fatalf("%s: %v", v.Name, err)
		}
		if env.EPK != v.Epk || env.CT != v.Ct || env.Tag != v.Tag || env.Ver != v.Ver {
			t.Errorf("%s: got %+v\nwant epk=%s ct=%s tag=%s ver=%d", v.Name, env, v.Epk, v.Ct, v.Tag, v.Ver)
		}
		secret, err := envelopeSecret(ephSeed(i+1), unhex(t, v.ObsPub))
		if err != nil || hex.EncodeToString(secret) != v.Secret {
			t.Errorf("%s: secret %x %v want %s", v.Name, secret, err, v.Secret)
		}
	}
}

func TestOpenReferenceReadsFirmwareVectors(t *testing.T) {
	good, bad := loadVectors(t)
	for _, v := range good {
		pw, err := OpenEnvelope(unhex(t, v.ObsPrv), unhex(t, v.ObsPub), unhex(t, v.Target), Envelope{EPK: v.Epk, CT: v.Ct, Tag: v.Tag, Ver: v.Ver})
		if err != nil || hex.EncodeToString(pw) != v.Pw {
			t.Errorf("%s: %x %v", v.Name, pw, err)
		}
		// The envelope's own secret, from the observer side, matches the vector.
		s, err := openerSecret(unhex(t, v.ObsPrv), unhex(t, v.Epk))
		if err != nil || hex.EncodeToString(s) != v.Secret {
			t.Errorf("%s: opener secret %x %v", v.Name, s, err)
		}
	}
	// Plaintexts the device refuses: empty, sixteen bytes, bytes after a NUL. Their tags are valid, so the
	// rule is in the plaintext check.
	for _, v := range bad {
		if _, err := openWithSecret(unhex(t, v.Secret), unhex(t, v.ObsPub), unhex(t, v.Target), Envelope{EPK: v.Epk, CT: v.Ct, Tag: v.Tag, Ver: v.Ver}); err == nil {
			t.Errorf("%s: opened", v.Name)
		}
	}
}

func TestSealRoundTripAndTamper(t *testing.T) {
	pub, prv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	// Expand the seed the way the device keeps its private key.
	exp := expandSeed(prv.Seed())
	target := bytes.Repeat([]byte{7}, 32)
	env, err := SealEnvelope(rand.Reader, pub, target, 5, []byte("hunter2"))
	if err != nil {
		t.Fatal(err)
	}
	if len(env.EPK) != 64 || len(env.CT) != 32 || len(env.Tag) != 64 || env.EPK != strings.ToLower(env.EPK) {
		t.Fatalf("shape %+v", env)
	}
	got, err := OpenEnvelope(exp, pub, target, env)
	if err != nil || string(got) != "hunter2" {
		t.Fatalf("round trip: %q %v", got, err)
	}
	env2, _ := SealEnvelope(rand.Reader, pub, target, 5, []byte("hunter2"))
	if env2.EPK == env.EPK || env2.CT == env.CT {
		t.Error("two envelopes share the one-time key")
	}

	flip := func(s string) string {
		b := []byte(s)
		if b[0] == '0' {
			b[0] = '1'
		} else {
			b[0] = '0'
		}
		return string(b)
	}
	other := bytes.Repeat([]byte{8}, 32)
	for name, f := range map[string]func() error{
		"tag":      func() error { e := env; e.Tag = flip(e.Tag); _, err := OpenEnvelope(exp, pub, target, e); return err },
		"ct":       func() error { e := env; e.CT = flip(e.CT); _, err := OpenEnvelope(exp, pub, target, e); return err },
		"epk":      func() error { e := env; e.EPK = flip(e.EPK); _, err := OpenEnvelope(exp, pub, target, e); return err },
		"ver":      func() error { e := env; e.Ver++; _, err := OpenEnvelope(exp, pub, target, e); return err },
		"target":   func() error { _, err := OpenEnvelope(exp, pub, other, env); return err },
		"observer": func() error { _, err := OpenEnvelope(exp, other, target, env); return err },
	} {
		if f() == nil {
			t.Errorf("tampered %s opened", name)
		}
	}
}

func TestSealRefusals(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	target := bytes.Repeat([]byte{7}, 32)
	for name, pw := range map[string][]byte{"empty": nil, "sixteen": bytes.Repeat([]byte{'a'}, 16), "nul": []byte("ab\x00c")} {
		if _, err := SealEnvelope(rand.Reader, pub, target, 1, pw); err == nil {
			t.Errorf("%s password sealed", name)
		}
	}
	if _, err := SealEnvelope(rand.Reader, pub, target, 1, bytes.Repeat([]byte{'a'}, 15)); err != nil {
		t.Errorf("15 bytes: %v", err)
	}
	if _, err := SealEnvelope(rand.Reader, pub[:31], target, 1, []byte("x")); err == nil {
		t.Error("short observer key")
	}
	if _, err := SealEnvelope(rand.Reader, pub, target[:5], 1, []byte("x")); err == nil {
		t.Error("short target key")
	}
	if _, err := SealEnvelope(bytes.NewReader([]byte{1, 2}), pub, target, 1, []byte("x")); err == nil {
		t.Error("short random source")
	}
	// An observer key whose Edwards y is 1 is the identity: the Montgomery map is undefined, so no secret.
	identity := make([]byte, 32)
	identity[0] = 1
	if _, err := SealEnvelope(rand.Reader, identity, target, 1, []byte("x")); err == nil {
		t.Error("identity observer key accepted")
	}
	// y = 0 maps to u = 1, whose shared secret is not all zero but y = -1 (u = 0) is a low-order point.
	lowOrder := bytes.Repeat([]byte{0xff}, 32)
	lowOrder[0], lowOrder[31] = 0xec, 0x7f
	if _, err := SealEnvelope(rand.Reader, lowOrder, target, 1, []byte("x")); err == nil {
		t.Error("low-order observer key accepted")
	}
}

func TestEnvelopeJSON(t *testing.T) {
	b, _ := json.Marshal(Envelope{EPK: "aa", CT: "bb", Tag: "cc", Ver: 4294967295})
	if string(b) != `{"epk":"aa","ct":"bb","tag":"cc","ver":4294967295}` {
		t.Fatalf("%s", b)
	}
}
