// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeStore struct {
	Store // unused methods panic
	ch    UserChannels
	dev   bool
	set   [2]*bool
	reg   struct {
		platform, token string
		key             []byte
	}
}

func (f *fakeStore) Targets(context.Context, uuid.UUID, time.Time) ([]TargetInfo, error) {
	return nil, nil
}
func (f *fakeStore) HasDevice(context.Context, uuid.UUID) (bool, error)            { return f.dev, nil }
func (f *fakeStore) UserChannels(context.Context, uuid.UUID) (UserChannels, error) { return f.ch, nil }
func (f *fakeStore) SetUserChannels(_ context.Context, _ uuid.UUID, email, push *bool) error {
	f.set = [2]*bool{email, push}
	if email != nil {
		f.ch.Email = *email
	}
	if push != nil {
		f.ch.Push = *push
	}
	return nil
}
func (f *fakeStore) RegisterDevice(_ context.Context, _, _ uuid.UUID, platform, token string, key []byte) error {
	f.reg.platform, f.reg.token, f.reg.key = platform, token, key
	f.ch.Push = true
	return nil
}

func TestChannelAvailability(t *testing.T) {
	verified := UserChannels{Email: true, Push: true, HasEmail: true, EmailVerified: true}
	for _, tc := range []struct {
		name   string
		server ServerChannels
		user   UserChannels
		dev    bool
		want   Channels
	}{
		{"both on", ServerChannels{Email: true, Push: true}, verified, true,
			Channels{Email: EmailChannel{Available: true, Enabled: true}, Push: PushChannel{Available: true, Enabled: true, HasDevice: true}}},
		{"server offers neither", ServerChannels{}, verified, false,
			Channels{Email: EmailChannel{Enabled: true, Reason: "server_off"}, Push: PushChannel{Enabled: true}}},
		{"portal", ServerChannels{Push: true, Portal: true}, UserChannels{Email: true, Push: false}, false,
			Channels{Email: EmailChannel{Enabled: true, Reason: "portal_account"}, Push: PushChannel{Available: true}}},
		{"unverified", ServerChannels{Email: true}, UserChannels{Email: true, HasEmail: true}, false,
			Channels{Email: EmailChannel{Enabled: true, Reason: "unverified_email"}, Push: PushChannel{}}},
		{"switched off", ServerChannels{Email: true, Push: true}, UserChannels{HasEmail: true, EmailVerified: true}, true,
			Channels{Email: EmailChannel{Available: true}, Push: PushChannel{Available: true, HasDevice: true}}},
	} {
		svc := NewService(&fakeStore{ch: tc.user, dev: tc.dev}, tc.server, nil)
		got, err := svc.Channels(context.Background(), uuid.New())
		if err != nil || got != tc.want {
			t.Errorf("%s: %+v %v, want %+v", tc.name, got, err, tc.want)
		}
		ov, _ := svc.Overview(context.Background(), uuid.New())
		if ov.Channels != tc.want {
			t.Errorf("%s overview: %+v", tc.name, ov.Channels)
		}
	}
}

func TestSetChannelsChangesOnlyWhatIsGiven(t *testing.T) {
	f := &fakeStore{ch: UserChannels{Email: true, Push: true, HasEmail: true, EmailVerified: true}}
	svc := NewService(f, ServerChannels{Email: true, Push: true}, nil)
	off := false
	got, err := svc.SetChannels(context.Background(), uuid.New(), nil, &off)
	if err != nil || f.set[0] != nil || f.set[1] == nil || got.Push.Enabled || !got.Email.Enabled {
		t.Fatalf("%+v %v %v", got, err, f.set)
	}
	if _, err := svc.SetChannels(context.Background(), uuid.New(), nil, nil); !errors.Is(err, ErrNoChannel) {
		t.Fatalf("%v", err)
	}
}

func TestRegisterDeviceNeedsAHandleAndAPayloadKey(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	f := &fakeStore{}
	svc := NewService(f, ServerChannels{Push: true}, nil)
	if err := svc.RegisterDevice(context.Background(), uuid.New(), uuid.New(), "ios", " h_abc123 ", key); err != nil {
		t.Fatal(err)
	}
	if f.reg.platform != "ios" || f.reg.token != "h_abc123" || len(f.reg.key) != 32 {
		t.Fatalf("%+v", f.reg)
	}
	for name, in := range map[string][3]string{
		"platform":   {"web", "h_abc", key},
		"no prefix":  {"ios", "fcm-token", key},
		"empty":      {"ios", "h_", key},
		"space":      {"ios", "h_a b", key},
		"long":       {"ios", "h_" + strings.Repeat("a", 300), key},
		"no key":     {"ios", "h_abc", ""},
		"short key":  {"ios", "h_abc", base64.StdEncoding.EncodeToString(make([]byte, 16))},
		"url base64": {"ios", "h_abc", base64.RawURLEncoding.EncodeToString(make([]byte, 32))},
		"not base64": {"ios", "h_abc", "!!!"},
	} {
		if err := svc.RegisterDevice(context.Background(), uuid.New(), uuid.New(), in[0], in[1], in[2]); !errors.Is(err, ErrBadDevice) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// Without push there is nothing to register for.
	email := NewService(f, ServerChannels{Email: true}, nil)
	if err := email.RegisterDevice(context.Background(), uuid.New(), uuid.New(), "ios", "h_abc", key); !errors.Is(err, ErrPushOff) {
		t.Fatalf("%v", err)
	}
}
