// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package users

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"

	"golang.org/x/crypto/argon2"
)

func TestHashAndVerifyPassword(t *testing.T) {
	h, err := HashPassword("correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(h, "$argon2id$v=19$m=19456,t=2,p=1$") || strings.Count(h, "$") != 5 {
		t.Fatalf("format: %s", h)
	}
	if ok, re := VerifyPassword(h, "correct horse battery"); !ok || re {
		t.Fatalf("good: ok=%v rehash=%v", ok, re)
	}
	if ok, _ := VerifyPassword(h, "correct horse batterY"); ok {
		t.Fatal("wrong password verified")
	}
	h2, _ := HashPassword("correct horse battery")
	if h == h2 {
		t.Fatal("salt must differ")
	}
}

func phc(m, tt, p uint32, salt, key []byte) string {
	e := base64.RawStdEncoding
	return fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s", m, tt, p, e.EncodeToString(salt), e.EncodeToString(key))
}

func TestVerifyPasswordOtherParametersNeedsRehash(t *testing.T) {
	salt := []byte("0123456789abcdef")
	key := argon2.IDKey([]byte("hunter2hunter2"), salt, 1, 8*1024, 1, 32)
	h := phc(8*1024, 1, 1, salt, key)
	ok, re := VerifyPassword(h, "hunter2hunter2")
	if !ok || !re {
		t.Fatalf("ok=%v rehash=%v", ok, re)
	}
	// A different key length also counts as old parameters.
	key16 := argon2.IDKey([]byte("hunter2hunter2"), salt, 2, 19456, 1, 16)
	if ok, re := VerifyPassword(phc(19456, 2, 1, salt, key16), "hunter2hunter2"); !ok || !re {
		t.Fatalf("key16 ok=%v rehash=%v", ok, re)
	}
}

func TestVerifyPasswordMalformedAndAbsurd(t *testing.T) {
	salt := []byte("0123456789abcdef")
	key := make([]byte, 32)
	cases := []string{
		"", "plain", "$argon2id$", "$argon2i$v=19$m=19456,t=2,p=1$AAAA$AAAA",
		"$argon2id$v=18$m=19456,t=2,p=1$" + base64.RawStdEncoding.EncodeToString(salt) + "$" + base64.RawStdEncoding.EncodeToString(key),
		"$argon2id$v=19$m=x,t=2,p=1$AAAA$AAAA", "$argon2id$v=19$m=19456,t=2$AAAA$AAAA",
		"$argon2id$v=19$m=19456,t=2,p=1,q=1$AAAA$AAAA", "$argon2id$v=19$m=19456,t=2,p=1$!!!!$AAAA",
		phc(262145, 2, 1, salt, key), phc(4294967295, 2, 1, salt, key), phc(19456, 11, 1, salt, key),
		phc(19456, 2, 9, salt, key), phc(19456, 0, 1, salt, key), phc(19456, 2, 0, salt, key),
		phc(19456, 2, 1, salt[:7], key), phc(19456, 2, 1, make([]byte, 65), key),
		phc(19456, 2, 1, salt, key[:7]), phc(19456, 2, 1, salt, make([]byte, 65)),
	}
	for _, c := range cases {
		if ok, re := VerifyPassword(c, "x"); ok || re {
			t.Errorf("%q: ok=%v rehash=%v", c, ok, re)
		}
	}
}

func TestPasswordPolicy(t *testing.T) {
	cases := []struct {
		name, pw, user, email string
		weak                  bool
	}{
		{"ok", "a fine long passphrase", "ridge", "r@example.org", false},
		{"12 bytes", "abcdefghij12", "zed", "z@example.org", false},
		{"11 bytes", "abcdefghij1", "zed", "z@example.org", true},
		{"128 bytes", strings.Repeat("ab", 64), "zed", "z@example.org", false},
		{"129 bytes", strings.Repeat("ab", 64) + "c", "zed", "z@example.org", true},
		{"repeated", strings.Repeat("x", 20), "zed", "z@example.org", true},
		{"repeated multibyte", strings.Repeat("é", 10), "zed", "z@example.org", true},
		{"has username", "my-RidgeWalker-pass", "ridgewalker", "q@example.org", true},
		{"has email local", "xx-Jane.Doe-xx-1", "other", "jane.doe@example.org", true},
		{"short local ignored", "abcdefghijklmnopq", "zed", "a@example.org", false},
		{"common", "PassWord1234", "zed", "z@example.org", true},
		{"common beacon", "MeshCore1234", "zed", "z@example.org", true},
	}
	for _, c := range cases {
		err := CheckPasswordPolicy(c.pw, c.user, c.email)
		if c.weak {
			var pe *PolicyError
			if !errors.As(err, &pe) || pe.Reason == "" || !errors.Is(err, ErrWeakPassword) {
				t.Errorf("%s: err = %v", c.name, err)
			}
		} else if err != nil {
			t.Errorf("%s: unexpected %v", c.name, err)
		}
	}
}

func TestPolicyReasonsDiffer(t *testing.T) {
	reasons := map[string]bool{}
	for _, pw := range []string{"short", strings.Repeat("x", 20), "contains-ridgewalker-x", "password1234"} {
		err := CheckPasswordPolicy(pw, "ridgewalker", "q@example.org")
		var pe *PolicyError
		if !errors.As(err, &pe) {
			t.Fatalf("%q: %v", pw, err)
		}
		reasons[pe.Reason] = true
	}
	if len(reasons) != 4 {
		t.Fatalf("reasons: %v", reasons)
	}
}

func TestCommonPasswordList(t *testing.T) {
	list := commonPasswords()
	if len(list) < 1000 {
		t.Fatalf("only %d entries", len(list))
	}
	for p := range list {
		if len(p) < 12 || p != strings.ToLower(p) || p != strings.TrimSpace(p) {
			t.Fatalf("bad entry %q", p)
		}
	}
	for _, p := range []string{"password1234", "qwertyuiop12", "123456789012", "iloveyou1234", "letmein12345", "administrator", "welcome12345", "meshcore1234", "beaconbeacon", "repeater1234"} {
		if !list[p] {
			t.Errorf("missing %q", p)
		}
	}
}

func TestCommonPasswordFileUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, l := range strings.Split(strings.TrimSuffix(commonPasswordsRaw, "\n"), "\n") {
		if seen[l] {
			t.Fatalf("duplicate %q", l)
		}
		seen[l] = true
	}
	if len(seen) != len(commonPasswords()) {
		t.Fatal("file lines and parsed set differ")
	}
}

func TestTokens(t *testing.T) {
	a, ah := NewToken()
	b, _ := NewToken()
	if a == b || len(a) != 43 {
		t.Fatalf("tokens %q %q", a, b)
	}
	if raw, err := base64.RawURLEncoding.DecodeString(a); err != nil || len(raw) != 32 {
		t.Fatalf("not 32 bytes of base64url: %v", err)
	}
	if string(HashToken(a)) != string(ah) || len(ah) != 32 {
		t.Fatal("hash mismatch")
	}
	if string(HashToken(b)) == string(ah) {
		t.Fatal("hashes collide")
	}
}

const (
	composed   = "paésword-passphraseé" // é as one character
	decomposed = "paésword-passphraseé"
)

// The same password typed composed or decomposed is one password.
func TestPasswordsAreNormalisedToNFC(t *testing.T) {
	h, err := HashPassword(composed)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _ := VerifyPassword(h, decomposed); !ok {
		t.Fatal("the decomposed form of the password was refused")
	}
	h, _ = HashPassword(decomposed)
	if ok, re := VerifyPassword(h, composed); !ok || re {
		t.Fatalf("composed against a decomposed hash: ok=%v rehash=%v", ok, re)
	}
	// No fallback: a hash made from the raw decomposed bytes does not verify (no such account exists).
	salt := []byte("0123456789abcdef")
	old := phc(argonMemoryKiB, argonTime, argonThreads, salt, argon2.IDKey([]byte(decomposed), salt, argonTime, argonMemoryKiB, argonThreads, argonKeyLen))
	if ok, _ := VerifyPassword(old, decomposed); ok {
		t.Fatal("an un-normalised hash verified: the fallback is back")
	}
	if ok, _ := VerifyPassword(old, "something else entirely"); ok {
		t.Fatal("wrong password verified")
	}
}

// The minimum counts characters after normalisation, the maximum stays 128 bytes; the policy sees the normalised form.
func TestPolicyCountsCharactersAfterNormalisation(t *testing.T) {
	twelve := "éàùèâêîôûëïü" // 12 characters, 24 bytes
	if err := CheckPasswordPolicy(twelve, "", ""); err != nil {
		t.Fatalf("12 characters: %v", err)
	}
	decomp := "éàùèâêîôûëïü"
	if err := CheckPasswordPolicy(decomp, "", ""); err != nil {
		t.Fatalf("the same 12 characters decomposed: %v", err)
	}
	err := CheckPasswordPolicy(twelve[:len(twelve)-2], "", "")
	var pe *PolicyError
	if !errors.As(err, &pe) || pe.Reason != "Password must be at least 12 characters." {
		t.Fatalf("11 characters: %v", err)
	}
	if err := CheckPasswordPolicy(strings.Repeat("\u00e9x", 50), "", ""); err == nil || !strings.Contains(err.Error(), "128 bytes") {
		t.Fatalf("over 128 bytes: %v", err)
	}
	if err := CheckPasswordPolicy("my-zoé-passphrase", "zoé", ""); err == nil {
		t.Fatal("a username in another normal form got through")
	}
}
