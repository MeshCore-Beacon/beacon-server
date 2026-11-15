// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package mail sends the plain-text messages accounts need.
package mail

import (
	"context"
	"sync"
)

// Message is one plain-text email. Text is UTF-8 with any line endings.
type Message struct {
	To, Subject, Text string
}

// Sender delivers a message or reports why it could not.
type Sender interface {
	Send(ctx context.Context, m Message) error
}

// Discard is a Sender that drops every message, for an instance that has no mail setup.
type Discard struct{}

func (Discard) Send(context.Context, Message) error { return nil }

// Recorder is a Sender for tests: it keeps what it was given and can be told to fail.
// The zero value is ready and safe for concurrent use.
type Recorder struct {
	mu   sync.Mutex
	msgs []Message
	err  error
}

// Send records m, or returns the error set with FailWith without recording.
func (r *Recorder) Send(_ context.Context, m Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.msgs = append(r.msgs, m)
	return nil
}

// FailWith makes later sends return err; nil makes them succeed again.
func (r *Recorder) FailWith(err error) {
	r.mu.Lock()
	r.err = err
	r.mu.Unlock()
}

// Messages returns a copy of what was sent so far.
func (r *Recorder) Messages() []Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Message(nil), r.msgs...)
}
