// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/background"
)

func TestAlertSenderNeedsSomethingToSendWith(t *testing.T) {
	pu, _ := url.Parse("https://beacon.example.org")
	// Push with no credentials is a startup error naming the setting and the variable.
	s, err := newAlertSender("push", nil, nil, pu, "")
	if err == nil || s != nil || !strings.Contains(err.Error(), "alerts.delivery") || !strings.Contains(err.Error(), "BEACON_PUSH_CREDENTIALS") {
		t.Fatalf("push without credentials: %v %v", s, err)
	}
	if tasks := background.AlertTasks(nil, s); len(tasks) != 0 {
		t.Fatalf("tasks without a sender: %d", len(tasks))
	}
	if s, err := newAlertSender("off", nil, nil, pu, ""); err != nil || s != nil {
		t.Fatalf("off: %v %v", s, err)
	}
	if s, err := newAlertSender("email", nil, nil, pu, ""); err != nil || s != nil {
		t.Fatalf("email without a mailer: %v %v", s, err)
	}
}

func TestAlertSenderPushCredentials(t *testing.T) {
	pu, _ := url.Parse("https://beacon.example.org")
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte("do-not-print-this"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newAlertSender("push", nil, nil, pu, bad); err == nil || strings.Contains(err.Error(), "do-not-print-this") || strings.Contains(err.Error(), dir) {
		t.Fatalf("%v", err)
	}
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	der, _ := x509.MarshalPKCS8PrivateKey(key)
	good := filepath.Join(dir, "good.json")
	body, _ := json.Marshal(map[string]string{"project_id": "p", "client_email": "e@p.iam", "private_key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))})
	if err := os.WriteFile(good, body, 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := newAlertSender("push", nil, nil, pu, good)
	if err != nil || s == nil {
		t.Fatalf("%v %v", s, err)
	}
}
