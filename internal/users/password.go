// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package users

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"golang.org/x/crypto/argon2"
	"golang.org/x/text/unicode/norm"
)

// Current argon2id parameters; a stored hash made with others is rehashed on the next login.
const (
	argonMemoryKiB = 19456
	argonTime      = 2
	argonThreads   = 1
	argonSaltLen   = 16
	argonKeyLen    = 32

	// Upper bounds on what a stored hash may ask of us, so a corrupt row cannot exhaust memory or CPU.
	maxArgonMemoryKiB = 256 * 1024
	maxArgonTime      = 10
	maxArgonThreads   = 8
	minSaltOrKey      = 8
	maxSaltOrKey      = 64

	minPasswordChars = 12 // characters (runes) after normalisation
	maxPasswordBytes = 128
	minLocalPartLen  = 3
)

// PolicyError is a password refusal with a reason fit to show the user. It matches ErrWeakPassword.
type PolicyError struct{ Reason string }

func (e *PolicyError) Error() string { return e.Reason }

func (e *PolicyError) Is(target error) bool { return target == ErrWeakPassword }

// normalize puts a password in Unicode NFC, so one password typed composed or decomposed is one password.
func normalize(password string) string { return norm.NFC.String(password) }

// HashPassword returns an argon2id PHC string with a fresh random salt, of the password in NFC.
func HashPassword(password string) (string, error) {
	password = normalize(password)
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("users: salt: %w", err)
	}
	key := argon2.IDKey([]byte(password), salt, argonTime, argonMemoryKiB, argonThreads, argonKeyLen)
	e := base64.RawStdEncoding
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s", argon2.Version, argonMemoryKiB, argonTime, argonThreads,
		e.EncodeToString(salt), e.EncodeToString(key)), nil
}

func paramUint(s, prefix string) (uint32, bool) {
	v, ok := strings.CutPrefix(s, prefix)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(v, 10, 32)
	return uint32(n), err == nil
}

// VerifyPassword checks password against a PHC string. It reads the parameters from the hash, refuses absurd ones
// before doing any work, and reports needsRehash when they differ from the current ones. A bad hash is just false.
func VerifyPassword(hash, password string) (ok bool, needsRehash bool) {
	parts := strings.Split(hash, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" || parts[2] != "v="+strconv.Itoa(argon2.Version) {
		return false, false
	}
	ps := strings.Split(parts[3], ",")
	if len(ps) != 3 {
		return false, false
	}
	m, ok1 := paramUint(ps[0], "m=")
	t, ok2 := paramUint(ps[1], "t=")
	p, ok3 := paramUint(ps[2], "p=")
	if !ok1 || !ok2 || !ok3 || m == 0 || m > maxArgonMemoryKiB || t == 0 || t > maxArgonTime || p == 0 || p > maxArgonThreads {
		return false, false
	}
	salt, err1 := base64.RawStdEncoding.DecodeString(parts[4])
	want, err2 := base64.RawStdEncoding.DecodeString(parts[5])
	if err1 != nil || err2 != nil || len(salt) < minSaltOrKey || len(salt) > maxSaltOrKey || len(want) < minSaltOrKey || len(want) > maxSaltOrKey {
		return false, false
	}
	got := argon2.IDKey([]byte(normalize(password)), salt, t, m, uint8(p), uint32(len(want)))
	if subtle.ConstantTimeCompare(got, want) != 1 {
		return false, false
	}
	rehash := m != argonMemoryKiB || t != argonTime || p != argonThreads || len(salt) != argonSaltLen || len(want) != argonKeyLen
	return true, rehash
}

//go:embed common_passwords.txt
var commonPasswordsRaw string

var (
	commonOnce sync.Once
	commonSet  map[string]bool
)

// commonPasswords is the bundled list as a set of lower-case entries.
func commonPasswords() map[string]bool {
	commonOnce.Do(func() {
		commonSet = map[string]bool{}
		for _, l := range strings.Split(commonPasswordsRaw, "\n") {
			if l = strings.TrimSpace(l); l != "" {
				commonSet[l] = true
			}
		}
	})
	return commonSet
}

func allSameRune(s string) bool {
	first, _ := utf8.DecodeRuneInString(s)
	for _, r := range s {
		if r != first {
			return false
		}
	}
	return true
}

// CheckPasswordPolicy returns a *PolicyError (matching ErrWeakPassword) when the password is refused.
// Empty username or email skip the matching check.
func CheckPasswordPolicy(password, username, email string) error {
	password, username, email = normalize(password), normalize(username), normalize(email)
	if utf8.RuneCountInString(password) < minPasswordChars {
		return &PolicyError{"Password must be at least 12 characters."}
	}
	if len(password) > maxPasswordBytes {
		return &PolicyError{"Password must be at most 128 bytes."}
	}
	if allSameRune(password) {
		return &PolicyError{"Password must not be one repeated character."}
	}
	lower := strings.ToLower(password)
	if username != "" && strings.Contains(lower, strings.ToLower(username)) {
		return &PolicyError{"Password must not contain your username."}
	}
	// A very short local part would match most passwords by chance.
	if local, _, found := strings.Cut(strings.ToLower(email), "@"); found && utf8.RuneCountInString(local) >= minLocalPartLen && strings.Contains(lower, local) {
		return &PolicyError{"Password must not contain the first part of your email address."}
	}
	if commonPasswords()[lower] {
		return &PolicyError{"That password is too common. Choose a different one."}
	}
	return nil
}

// NewToken returns 32 random bytes as base64url and the SHA-256 hash that is stored.
func NewToken() (plain string, hash []byte) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic("users: crypto/rand failed: " + err.Error())
	}
	plain = base64.RawURLEncoding.EncodeToString(b)
	return plain, HashToken(plain)
}

// HashToken is SHA-256 of the plain token's bytes.
func HashToken(plain string) []byte {
	h := sha256.Sum256([]byte(plain))
	return h[:]
}
