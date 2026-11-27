// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func open(t *testing.T, key []byte, pushID string, ct []byte) []byte {
	t.Helper()
	block, _ := aes.NewCipher(key)
	gcm, _ := cipher.NewGCM(block)
	if len(ct) < 12+16 {
		t.Fatalf("ciphertext too short: %d", len(ct))
	}
	pt, err := gcm.Open(nil, ct[:12], ct[12:], []byte("beacon-push/v1:"+pushID))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return pt
}

func TestNoticePayloadIsTheStructuredNotice(t *testing.T) {
	id := uuid.MustParse("6f1c1c7e-8c1a-4a57-9a51-0d6a3f4f2b10")
	r := row(1, userA, passOne, KindBatteryLow, "Hill\ntop", 3412)
	r.TargetID = id
	got := string(NoticePayload(r, "c"))
	want := `{"v":1,"kind":"battery_low","name":"Hill top","value":3412,"detail":"","targetKind":"node",` +
		`"publicKey":"0100000000000000000000000000000000000000000000000000000000000000","id":"` + id.String() + `","tempUnit":"c"}`
	if got != want {
		t.Fatalf("\n got %s\nwant %s", got, want)
	}
	// No value is null, an unknown radio has an empty id, and the unit follows the instance.
	r = row(2, userA, passOne, KindBackOnline, "", 0)
	r.HasValue = false
	r.Detail = "x"
	var m map[string]any
	if err := json.Unmarshal(NoticePayload(r, "f"), &m); err != nil {
		t.Fatal(err)
	}
	if v, ok := m["value"]; !ok || v != nil || m["id"] != "" || m["tempUnit"] != "f" || m["name"] != "02000000" || m["detail"] != "x" {
		t.Fatalf("%v", m)
	}
}

// The evaluator stores a number for every notice; the kinds without a figure still send null.
func TestNoticePayloadValuelessKindsAreNull(t *testing.T) {
	for _, kind := range []string{KindBackOnline, KindRebooted, KindRequestAttention} {
		var m map[string]any
		if err := json.Unmarshal(NoticePayload(row(1, userA, passOne, kind, "A", 5400), "c"), &m); err != nil {
			t.Fatal(err)
		}
		if v, ok := m["value"]; !ok || v != nil {
			t.Errorf("%s: value %v", kind, m["value"])
		}
	}
	var m map[string]any
	_ = json.Unmarshal(NoticePayload(row(1, userA, passOne, KindGoneQuiet, "A", 0), "c"), &m)
	if m["value"] != float64(0) {
		t.Fatalf("a zero figure is still a figure: %v", m["value"])
	}
}

func TestSealNoticeRoundTripsWithThePushIDBound(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	pt := []byte(`{"v":1}`)
	ct, err := SealNotice(key, "inst_abc", pt)
	if err != nil {
		t.Fatal(err)
	}
	if len(ct) != 12+len(pt)+16 {
		t.Fatalf("length %d", len(ct))
	}
	if got := open(t, key, "inst_abc", ct); !bytes.Equal(got, pt) {
		t.Fatalf("%q", got)
	}
	// Another server's id does not open it.
	block, _ := aes.NewCipher(key)
	gcm, _ := cipher.NewGCM(block)
	if _, err := gcm.Open(nil, ct[:12], ct[12:], []byte("beacon-push/v1:inst_other")); err == nil {
		t.Fatal("opened with another push id")
	}
	// A fresh nonce every time.
	ct2, _ := SealNotice(key, "inst_abc", pt)
	if bytes.Equal(ct[:12], ct2[:12]) {
		t.Fatal("nonce reused")
	}
	if _, err := SealNotice(key[:16], "inst_abc", pt); err == nil {
		t.Fatal("a 16-byte key was accepted")
	}
}

// The relay refuses ciphertext over 2,048 bytes; the biggest notice a radio can shape must fit with room to spare.
func TestLargestNoticeFitsThePushServerCap(t *testing.T) {
	key := bytes.Repeat([]byte{1}, 32)
	for _, name := range []string{strings.Repeat("\U0001F4E1", 500), strings.Repeat(`<"\>`, 200), strings.Repeat(" ", 100)} {
		r := row(1, userA, passOne, KindRequestAttention, name, -9223372036854775808)
		r.TargetID = uuid.New()
		r.TargetKind = TargetObserver
		r.Detail = "admin_stopped"
		ct, err := SealNotice(key, strings.Repeat("i", 64), NoticePayload(r, "f"))
		if err != nil {
			t.Fatal(err)
		}
		if len(ct) > MaxCiphertext/2 {
			t.Fatalf("notice of %d bytes is too close to the %d byte cap", len(ct), MaxCiphertext)
		}
	}
}
