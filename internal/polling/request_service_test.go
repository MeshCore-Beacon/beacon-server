// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
)

// memRequests is an in-memory RequestStore that applies the same mutate contract as the database one.
type memRequests struct {
	nodes   map[uuid.UUID]RequestNode
	states  map[string]*TargetState
	owners  map[uuid.UUID][]byte // request id -> target
	elected [][]byte
	failFn  error
}

func newMem() *memRequests {
	return &memRequests{nodes: map[uuid.UUID]RequestNode{}, states: map[string]*TargetState{}, owners: map[uuid.UUID][]byte{}}
}

func (m *memRequests) RequestNode(_ context.Context, id uuid.UUID) (RequestNode, error) {
	n, ok := m.nodes[id]
	if !ok {
		return RequestNode{}, ErrRequestNotFound
	}
	return n, nil
}

func (m *memRequests) userCount(u uuid.UUID) int {
	n := 0
	for _, st := range m.states {
		for _, r := range st.Requests {
			if r.UserID == u {
				n++
			}
		}
	}
	return n
}

func (m *memRequests) MutateRequestTarget(_ context.Context, target []byte, userID uuid.UUID, fn func(*TargetState) error) error {
	cur := m.states[string(target)]
	if cur == nil {
		cur = &TargetState{Target: target}
	}
	// Work on a copy so an error stores nothing.
	cp := *cur
	cp.Requests = append([]Request(nil), cur.Requests...)
	cp.UserRequests = m.userCount(userID)
	if err := fn(&cp); err != nil {
		return err
	}
	m.states[string(target)] = &cp
	for _, r := range cp.Requests {
		m.owners[r.ID] = target
	}
	for _, id := range cp.Deleted {
		delete(m.owners, id)
	}
	cp.Deleted, cp.NewAdminStops, cp.TouchedOther = nil, nil, nil
	return nil
}

func (m *memRequests) UserRequestTarget(_ context.Context, userID, id uuid.UUID) ([]byte, error) {
	t, ok := m.owners[id]
	if !ok {
		return nil, ErrRequestNotFound
	}
	for _, r := range m.states[string(t)].Requests {
		if r.ID == id && r.UserID == userID {
			return t, nil
		}
	}
	return nil, ErrRequestNotFound
}

func (m *memRequests) UserTelemetryRequests(_ context.Context, userID uuid.UUID) ([]RequestRow, error) {
	var out []RequestRow
	for k, st := range m.states {
		for _, r := range st.Requests {
			if r.UserID == userID {
				out = append(out, RequestRow{ID: r.ID, Target: []byte(k), Status: r.Status, Reason: r.Reason, HasPassword: r.HasPassword})
			}
		}
	}
	return out, nil
}

func (m *memRequests) ElectTarget(_ context.Context, target []byte) error {
	m.elected = append(m.elected, target)
	return m.failFn
}

func testRequestService(t *testing.T, limit int) (*RequestService, *memRequests, *Sealer) {
	t.Helper()
	m := newMem()
	sealer, err := NewSealer(bytes.Repeat([]byte{4}, 32))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	return NewRequestService(m, sealer, m, limit, func() time.Time { now = now.Add(time.Minute); return now }, slog.New(slog.NewTextHandler(io.Discard, nil))), m, sealer
}

func (m *memRequests) addNode(kind int16, key byte) uuid.UUID {
	id := uuid.New()
	m.nodes[id] = RequestNode{PublicKey: bytes.Repeat([]byte{key}, 32), NodeType: kind, Name: "n"}
	return id
}

func TestRequestServiceCreate(t *testing.T) {
	ctx := context.Background()
	s, m, sealer := testRequestService(t, 2)
	user := uuid.New()
	rep, room, companion := m.addNode(2, 1), m.addNode(3, 2), m.addNode(1, 3)

	row, err := s.Create(ctx, user, rep, []byte("guest"))
	if err != nil || row.Status != StatusWaiting || !row.HasPassword {
		t.Fatalf("create: %+v %v", row, err)
	}
	// The stored password is sealed, bound to this request and repeater, and never plain.
	st := m.states[string(bytes.Repeat([]byte{1}, 32))]
	r := st.Requests[0]
	if bytes.Contains(r.PasswordCT, []byte("guest")) {
		t.Fatal("password stored in clear")
	}
	if pw, err := sealer.Open(r.ID, st.Target, r.PasswordCT, r.PasswordNonce); err != nil || string(pw) != "guest" {
		t.Fatalf("sealed password: %q %v", pw, err)
	}
	if len(m.elected) != 1 || !bytes.Equal(m.elected[0], st.Target) {
		t.Fatalf("not elected: %x", m.elected)
	}

	if _, err := s.Create(ctx, user, rep, nil); !errors.Is(err, ErrAlreadyRequested) {
		t.Errorf("duplicate: %v", err)
	}
	if row, err := s.Create(ctx, user, room, nil); err != nil || row.HasPassword {
		t.Errorf("room server without a password: %+v %v", row, err)
	}
	// At the limit: a new repeater is refused, and the duplicate answer wins when both apply.
	if _, err := s.Create(ctx, user, m.addNode(2, 9), nil); !errors.Is(err, ErrRequestLimit) {
		t.Errorf("limit: %v", err)
	}
	if _, err := s.Create(ctx, user, rep, nil); !errors.Is(err, ErrAlreadyRequested) {
		t.Errorf("duplicate at the limit: %v", err)
	}
	if _, err := s.Create(ctx, uuid.New(), companion, nil); !errors.Is(err, ErrNotARepeater) {
		t.Errorf("companion: %v", err)
	}
	if _, err := s.Create(ctx, uuid.New(), uuid.New(), nil); !errors.Is(err, ErrRequestNotFound) {
		t.Errorf("unknown node: %v", err)
	}
	for name, pw := range map[string][]byte{"sixteen": bytes.Repeat([]byte{'a'}, 16), "nul": []byte("a\x00b"), "empty": {}} {
		if _, err := s.Create(ctx, uuid.New(), m.addNode(2, 20), pw); !errors.Is(err, ErrPasswordInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// An election failure does not undo the request.
	m.failFn = errors.New("db down")
	if _, err := s.Create(ctx, uuid.New(), m.addNode(2, 21), nil); err != nil {
		t.Errorf("election failure surfaced: %v", err)
	}
}

func TestRequestServiceUpdateAndDelete(t *testing.T) {
	ctx := context.Background()
	s, m, sealer := testRequestService(t, 3)
	user, other := uuid.New(), uuid.New()
	node := m.addNode(2, 1)
	target := bytes.Repeat([]byte{1}, 32)
	row, _ := s.Create(ctx, user, node, []byte("old"))
	m.elected = nil

	// A new password is sealed again under the same id.
	if _, err := s.Update(ctx, user, row.ID, RequestUpdate{SetPassword: true, Password: []byte("new")}); err != nil {
		t.Fatal(err)
	}
	st := m.states[string(target)]
	if pw, err := sealer.Open(row.ID, target, st.Requests[0].PasswordCT, st.Requests[0].PasswordNonce); err != nil || string(pw) != "new" {
		t.Fatalf("replaced: %q %v", pw, err)
	}
	if st.Ver < 2 {
		t.Errorf("ver did not rise on a new password: %d", st.Ver)
	}
	if len(m.elected) != 1 {
		t.Error("update was not elected")
	}
	// A password that does not fit changes nothing.
	if _, err := s.Update(ctx, user, row.ID, RequestUpdate{SetPassword: true, Password: bytes.Repeat([]byte{'a'}, 16)}); !errors.Is(err, ErrPasswordInvalid) {
		t.Errorf("bad password: %v", err)
	}
	// Removing the password.
	if r, err := s.Update(ctx, user, row.ID, RequestUpdate{SetPassword: true}); err != nil || r.HasPassword {
		t.Errorf("remove password: %+v %v", r, err)
	}
	// Somebody else's request is not found, whatever the id.
	if _, err := s.Update(ctx, other, row.ID, RequestUpdate{Enable: true}); !errors.Is(err, ErrRequestNotFound) {
		t.Errorf("other user update: %v", err)
	}
	if err := s.Delete(ctx, other, row.ID); !errors.Is(err, ErrRequestNotFound) {
		t.Errorf("other user delete: %v", err)
	}
	if err := s.Delete(ctx, user, row.ID); err != nil {
		t.Fatal(err)
	}
	if len(m.states[string(target)].Requests) != 0 || m.states[string(target)].Requested() {
		t.Error("request not removed")
	}
	if err := s.Delete(ctx, user, row.ID); !errors.Is(err, ErrRequestNotFound) {
		t.Errorf("second delete: %v", err)
	}
	if s.Limit() != 3 {
		t.Error("limit")
	}
}
