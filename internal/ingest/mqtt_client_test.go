// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
)

// Run with -race: /brokers reads the client while Start creates it.
func TestIsConnectedWhileStarting(t *testing.T) {
	w := New(Config{BrokerName: "test", URL: "tcp://127.0.0.1:1"}, &stubDB{}, hub.New(), &stubKeys{}, &stubScopes{})
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { w.Start(ctx); close(stopped) }()
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		w.IsConnected()
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}
}
