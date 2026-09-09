// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"net"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

type mqttConnection struct {
	id   string
	conn net.Conn
}

// A local protocol fixture observes the actual CONNECT packets sent by Start.
// It neither publishes nor connects to a real broker.
func mqttTestBroker(t *testing.T) (string, <-chan mqttConnection) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	connected := make(chan mqttConnection, 4)
	var clients sync.WaitGroup
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			clients.Add(1)
			go func() {
				defer clients.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				first, err := packets.ReadPacket(conn)
				if err != nil {
					return
				}
				connect, ok := first.(*packets.ConnectPacket)
				if !ok {
					return
				}
				if err := packets.NewControlPacket(packets.Connack).Write(conn); err != nil {
					return
				}
				announced := false
				for {
					packet, err := packets.ReadPacket(conn)
					if err != nil {
						return
					}
					switch packet := packet.(type) {
					case *packets.SubscribePacket:
						ack := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
						ack.MessageID = packet.MessageID
						ack.ReturnCodes = []byte{1}
						if err := ack.Write(conn); err != nil {
							return
						}
						if !announced {
							connected <- mqttConnection{connect.ClientIdentifier, conn}
							announced = true
						}
					case *packets.PingreqPacket:
						if err := packets.NewControlPacket(packets.Pingresp).Write(conn); err != nil {
							return
						}
					case *packets.DisconnectPacket:
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); <-stopped; clients.Wait() })
	return "tcp://" + listener.Addr().String(), connected
}

func startMQTTTestWorker(t *testing.T, address string) {
	t.Helper()
	w, _ := newTestWorker()
	w.cfg.URL = address
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); w.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("worker did not stop")
		}
	})
}

func awaitMQTTConnection(t *testing.T, connected <-chan mqttConnection) mqttConnection {
	t.Helper()
	select {
	case c := <-connected:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("MQTT CONNECT not received")
		return mqttConnection{}
	}
}

func TestWorkersUseDistinctMQTTClientIDs(t *testing.T) {
	address, connected := mqttTestBroker(t)
	startMQTTTestWorker(t, address)
	first := awaitMQTTConnection(t, connected)
	startMQTTTestWorker(t, address)
	second := awaitMQTTConnection(t, connected)
	if first.id == "" || second.id == "" || first.id == second.id {
		t.Fatalf("workers with the same broker name must use distinct client IDs: %q and %q", first.id, second.id)
	}
	valid := regexp.MustCompile(`^[A-Za-z0-9]{1,23}$`)
	if !valid.MatchString(first.id) || !valid.MatchString(second.id) {
		t.Fatal("client ID must be 1..23 alphanumeric characters")
	}
}

func TestReconnectKeepsMQTTClientID(t *testing.T) {
	address, connected := mqttTestBroker(t)
	startMQTTTestWorker(t, address)
	first := awaitMQTTConnection(t, connected)
	_ = first.conn.Close()
	second := awaitMQTTConnection(t, connected)
	if first.id != second.id {
		t.Fatalf("reconnect changed client ID: %q to %q", first.id, second.id)
	}
}
