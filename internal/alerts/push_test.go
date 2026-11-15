// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type fcmServer struct {
	t         *testing.T
	key       *rsa.PrivateKey
	srv       *httptest.Server
	mu        sync.Mutex
	tokenHits int
	sendHits  int
	lastSend  map[string]any
	lastAuth  string
	lastPath  string
	sendReply func(w http.ResponseWriter, hit int)
	expiresIn int
}

func newFCMServer(t *testing.T) *fcmServer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	f := &fcmServer{t: t, key: key, expiresIn: 3600}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case r.URL.Path == "/token":
			f.tokenHits++
			_ = r.ParseForm()
			f.checkAssertion(r.PostForm.Get("assertion"), r.PostForm.Get("grant_type"))
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "access-" + string(rune('0'+f.tokenHits)), "expires_in": f.expiresIn})
		case strings.HasSuffix(r.URL.Path, "/messages:send"):
			f.sendHits++
			f.lastAuth, f.lastPath = r.Header.Get("Authorization"), r.URL.Path
			body, _ := io.ReadAll(r.Body)
			f.lastSend = map[string]any{}
			_ = json.Unmarshal(body, &f.lastSend)
			if f.sendReply != nil {
				f.sendReply(w, f.sendHits)
				return
			}
			_, _ = w.Write([]byte(`{"name":"projects/p/messages/1"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fcmServer) checkAssertion(jwt, grant string) {
	f.t.Helper()
	if grant != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
		f.t.Errorf("grant type %q", grant)
	}
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		f.t.Errorf("assertion %q", jwt)
		return
	}
	sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(&f.key.PublicKey, crypto.SHA256, sum[:], sig); err != nil {
		f.t.Errorf("assertion signature: %v", err)
	}
	hdr, _ := base64.RawURLEncoding.DecodeString(parts[0])
	claims, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var c map[string]any
	_ = json.Unmarshal(claims, &c)
	if !strings.Contains(string(hdr), "RS256") || c["iss"] != "svc@project.iam" || c["scope"] != "https://www.googleapis.com/auth/firebase.messaging" ||
		c["aud"] != f.srv.URL+"/token" || c["exp"].(float64)-c["iat"].(float64) != 3600 {
		f.t.Errorf("claims %s header %s", claims, hdr)
	}
}

func (f *fcmServer) fcm(now func() time.Time) *FCM {
	return NewFCM("my-project", "svc@project.iam", f.srv.URL+"/token", f.srv.URL, f.key, f.srv.Client(), now)
}

var sampleMsg = PushMessage{Token: "device-token-abc", Title: "Hilltop", Body: "Hilltop rebooted.", Data: map[string]string{"targetKind": "node", "publicKey": "ab"}}

func TestFCMSendsTheV1Message(t *testing.T) {
	f := newFCMServer(t)
	if err := f.fcm(nil).Send(context.Background(), sampleMsg); err != nil {
		t.Fatal(err)
	}
	m := f.lastSend["message"].(map[string]any)
	data := m["data"].(map[string]any)
	note := m["notification"].(map[string]any)
	if f.lastPath != "/v1/projects/my-project/messages:send" || f.lastAuth != "Bearer access-1" || m["token"] != "device-token-abc" ||
		note["title"] != "Hilltop" || note["body"] != "Hilltop rebooted." || len(data) != 2 || data["targetKind"] != "node" || data["publicKey"] != "ab" {
		t.Fatalf("%s %s %+v", f.lastPath, f.lastAuth, f.lastSend)
	}
}

func TestFCMCachesTheAccessTokenUntilNearExpiry(t *testing.T) {
	f := newFCMServer(t)
	clock := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	c := f.fcm(func() time.Time { return clock })
	for i := 0; i < 3; i++ {
		if err := c.Send(context.Background(), sampleMsg); err != nil {
			t.Fatal(err)
		}
	}
	if f.tokenHits != 1 || f.sendHits != 3 {
		t.Fatalf("token hits %d send hits %d", f.tokenHits, f.sendHits)
	}
	clock = clock.Add(59 * time.Minute) // inside the two minute refresh margin of a one hour token
	if err := c.Send(context.Background(), sampleMsg); err != nil {
		t.Fatal(err)
	}
	if f.tokenHits != 2 || f.lastAuth != "Bearer access-2" {
		t.Fatalf("not refreshed: hits %d auth %s", f.tokenHits, f.lastAuth)
	}
}

func TestFCMUnregisteredToken(t *testing.T) {
	f := newFCMServer(t)
	f.sendReply = func(w http.ResponseWriter, _ int) {
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"error":{"code":404,"status":"NOT_FOUND","details":[{"@type":"x","errorCode":"UNREGISTERED"}]}}`))
	}
	if err := f.fcm(nil).Send(context.Background(), sampleMsg); !errors.Is(err, ErrUnregistered) {
		t.Fatalf("%v", err)
	}
	f.sendReply = func(w http.ResponseWriter, _ int) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"error":{"code":400,"status":"INVALID_ARGUMENT","details":[{"errorCode":"UNREGISTERED"}]}}`))
	}
	if err := f.fcm(nil).Send(context.Background(), sampleMsg); !errors.Is(err, ErrUnregistered) {
		t.Fatalf("400 unregistered: %v", err)
	}
	f.sendReply = func(w http.ResponseWriter, _ int) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"bad payload"}}`))
	}
	if err := f.fcm(nil).Send(context.Background(), sampleMsg); err == nil || errors.Is(err, ErrUnregistered) {
		t.Fatalf("a plain 400 must be a failure, not an unregistered token: %v", err)
	}
}

func TestFCMRetriesOnceAfterAStaleAccessToken(t *testing.T) {
	f := newFCMServer(t)
	f.sendReply = func(w http.ResponseWriter, hit int) {
		if hit == 1 {
			w.WriteHeader(401)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}
	if err := f.fcm(nil).Send(context.Background(), sampleMsg); err != nil {
		t.Fatal(err)
	}
	if f.tokenHits != 2 || f.sendHits != 2 || f.lastAuth != "Bearer access-2" {
		t.Fatalf("token hits %d, send hits %d, auth %s", f.tokenHits, f.sendHits, f.lastAuth)
	}
	f.sendReply = func(w http.ResponseWriter, _ int) { w.WriteHeader(401) }
	if err := f.fcm(nil).Send(context.Background(), sampleMsg); err == nil {
		t.Fatal("a second 401 must fail")
	}
}

func TestFCMErrorsNeverCarrySecrets(t *testing.T) {
	f := newFCMServer(t)
	f.sendReply = func(w http.ResponseWriter, _ int) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`{"error":"device-token-abc access-1 leaked"}`))
	}
	err := f.fcm(nil).Send(context.Background(), sampleMsg)
	if err == nil || strings.Contains(err.Error(), "device-token-abc") || strings.Contains(err.Error(), "access-1") || !strings.Contains(err.Error(), "500") {
		t.Fatalf("%v", err)
	}
	// An unreachable token endpoint says so without the URL, the assertion or the key.
	c := NewFCM("p", "svc@project.iam", "http://127.0.0.1:1/token", "http://127.0.0.1:1", f.key, nil, nil)
	err = c.Send(context.Background(), sampleMsg)
	if err == nil || strings.Contains(err.Error(), "127.0.0.1") || strings.Contains(err.Error(), "device-token-abc") {
		t.Fatalf("%v", err)
	}
}

func serviceAccountJSON(t *testing.T, key *rsa.PrivateKey, tokenURI string) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	b, _ := json.Marshal(map[string]string{"project_id": "my-project", "client_email": "svc@project.iam", "private_key": string(pemKey), "token_uri": tokenURI})
	return b
}

func TestLoadFCM(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	dir := t.TempDir()
	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	if _, err := LoadFCM(write("ok.json", serviceAccountJSON(t, key, "https://oauth2.example/token"))); err != nil {
		t.Fatal(err)
	}
	// No token_uri falls back to Google's.
	if f, err := LoadFCM(write("default.json", serviceAccountJSON(t, key, ""))); err != nil || f.tokenURL != "https://oauth2.googleapis.com/token" {
		t.Fatalf("%v", err)
	}
	for name, content := range map[string][]byte{
		"http token":   serviceAccountJSON(t, key, "http://oauth2.example/token"),
		"not json":     []byte("hunter2-secret"),
		"missing key":  []byte(`{"project_id":"p","client_email":"e"}`),
		"bad pem":      []byte(`{"project_id":"p","client_email":"e","private_key":"super-secret-material"}`),
		"empty object": []byte(`{}`),
	} {
		_, err := LoadFCM(write(name+".json", content))
		if err == nil {
			t.Errorf("%s accepted", name)
			continue
		}
		for _, secret := range []string{"hunter2-secret", "super-secret-material", dir} {
			if strings.Contains(err.Error(), secret) {
				t.Errorf("%s: error leaks %q: %v", name, secret, err)
			}
		}
	}
	if _, err := LoadFCM(filepath.Join(dir, "missing.json")); err == nil || strings.Contains(err.Error(), dir) {
		t.Errorf("missing file: %v", err)
	}
}
