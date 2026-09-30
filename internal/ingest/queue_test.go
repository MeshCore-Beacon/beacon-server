// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type queuedTestMessage struct {
	malformedTopicMessage
	payload []byte
}

func (m queuedTestMessage) Payload() []byte { return m.payload }
func queueMessage(observer, kind string, n int) mqtt.Message {
	return queuedTestMessage{malformedTopicMessage{"meshcore/SEA/" + observer + "/" + kind}, []byte(fmt.Sprint(n))}
}

func TestQueueKeepsObserverOrderWhileDatabaseIsBusy(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var got []string
	q := newMessageQueue(2, 8, 4096, func(ctx context.Context, m mqtt.Message) {
		if string(m.Payload()) == "0" {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return
			}
		}
		mu.Lock()
		got = append(got, string(m.Payload()))
		mu.Unlock()
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = q.close(ctx)
	})
	if !q.enqueue(queueMessage("aa", "status", 0)) {
		t.Fatal("first message rejected")
	}
	<-entered
	for n := 1; n <= 3; n++ {
		if !q.enqueue(queueMessage("AA", "packets", n)) {
			t.Fatal("callback blocked or message rejected")
		}
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := q.close(ctx); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"0", "1", "2", "3"}) {
		t.Fatalf("observer order: %v", got)
	}
	if q.enqueue(queueMessage("aa", "packets", 4)) {
		t.Fatal("accepted after shutdown")
	}
}

func TestQueueRunsOtherObserversAndReportsOverflow(t *testing.T) {
	entered, other := make(chan struct{}), make(chan struct{})
	q := newMessageQueue(2, 1, 4096, func(ctx context.Context, m mqtt.Message) {
		if m.Topic() == "meshcore/SEA/a/packets" {
			close(entered)
			<-ctx.Done()
		} else {
			close(other)
		}
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = q.close(ctx)
	})
	q.enqueue(queueMessage("a", "packets", 0))
	<-entered
	if !q.enqueue(queueMessage("b", "packets", 0)) {
		t.Fatal("other observer rejected")
	}
	select {
	case <-other:
	case <-time.After(time.Second):
		t.Fatal("other observer blocked")
	}
	if !q.enqueue(queueMessage("a", "packets", 1)) {
		t.Fatal("buffered message rejected")
	}
	if q.enqueue(queueMessage("a", "packets", 2)) {
		t.Fatal("accepted beyond queue capacity")
	}
	if q.takeDropped() != 1 {
		t.Fatal("overflow was not counted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if q.close(ctx) == nil {
		t.Fatal("expected shutdown deadline")
	}
}

func TestQueueBoundsPayloadMemory(t *testing.T) {
	q := newMessageQueue(1, 8, 64, func(context.Context, mqtt.Message) {})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = q.close(ctx)
	})
	m := queuedTestMessage{malformedTopicMessage{"meshcore/SEA/a/packets"}, make([]byte, 65)}
	if q.enqueue(m) {
		t.Fatal("accepted oversized payload")
	}
	if q.takeDropped() != 1 {
		t.Fatal("memory overflow was not counted")
	}
}

type subscribeCapture struct {
	mqtt.Client
	callback mqtt.MessageHandler
}

func (c *subscribeCapture) Subscribe(_ string, _ byte, callback mqtt.MessageHandler) mqtt.Token {
	c.callback = callback
	return completedSubscribe{}
}

type completedSubscribe struct{ mqtt.Token }

func (completedSubscribe) Wait() bool   { return true }
func (completedSubscribe) Error() error { return nil }

func TestSubscribeCountsRejectedMessage(t *testing.T) {
	w, _ := newTestWorker()
	q := newMessageQueue(1, 1, 1, func(context.Context, mqtt.Message) { t.Error("rejected message processed") })
	defer q.close(context.Background())
	client := &subscribeCapture{}
	w.subscribe(client, q)
	client.callback(client, queueMessage("aa", "packets", 0))
	if q.takeDropped() != 1 {
		t.Fatal("rejected message was not counted")
	}
}

func TestQueueShutdownWaitsForCanceledHandler(t *testing.T) {
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	q := newMessageQueue(1, 1, 4096, func(ctx context.Context, _ mqtt.Message) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
	})
	q.enqueue(queueMessage("aa", "packets", 0))
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- q.close(ctx) }()
	<-canceled
	select {
	case <-stopped:
		close(release)
		t.Fatal("shutdown returned while handler was still running")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish")
	}
}
