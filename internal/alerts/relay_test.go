// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type sendBody struct {
	Messages []struct {
		Handle      string `json:"handle"`
		Ciphertext  string `json:"ciphertext"`
		CollapseKey string `json:"collapseKey"`
		TTLSeconds  int    `json:"ttlSeconds"`
		ID          string `json:"id"`
	} `json:"messages"`
}

func relayServer(t *testing.T, h http.HandlerFunc) *Relay {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return NewRelay(srv.URL, "inst_abc", "s3cret", nil)
}

func TestRelaySendPostsTheBatchAndMapsEachResult(t *testing.T) {
	var got sendBody
	r := relayServer(t, func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost || req.URL.Path != "/v1/send" || req.Header.Get("Authorization") != "Bearer inst_abc.s3cret" ||
			req.Header.Get("Content-Type") != "application/json" {
			t.Errorf("%s %s %q", req.Method, req.URL.Path, req.Header.Get("Authorization"))
		}
		_ = json.NewDecoder(req.Body).Decode(&got)
		w.Header().Set("Retry-After", "30")
		_, _ = io.WriteString(w, `{"results":[{"messageId":"m_1","status":"ok"},{"messageId":"m_2","status":"invalid_handle"},
			{"messageId":"m_3","status":"rate_limited"},{"messageId":"m_4","status":"failed"},{"messageId":"m_5","status":"too_large"},
			{"messageId":"m_6","status":"invalid"},{"messageId":"m_7","status":"something_new"},
			{"status":"rate_limited","retryAfterSeconds":5},{"status":"failed","retryAfterSeconds":90}]}`)
	})
	msgs := make([]PushMessage, 9)
	for i := range msgs {
		msgs[i] = PushMessage{Handle: "h_" + string(rune('a'+i)), Ciphertext: []byte{byte(i), 0xff}, CollapseKey: "42", ID: "42:s" + string(rune('a'+i))}
	}
	res, err := r.Send(context.Background(), msgs)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Messages) != 9 || got.Messages[0].Handle != "h_a" || got.Messages[0].CollapseKey != "42" || got.Messages[0].TTLSeconds != 86400 ||
		got.Messages[1].ID != "42:sb" ||
		got.Messages[1].Ciphertext != base64.StdEncoding.EncodeToString([]byte{1, 0xff}) {
		t.Fatalf("%+v", got)
	}
	want := []PushStatus{PushDelivered, PushGone, PushLimited, PushRetry, PushDropped, PushDropped, PushRetry, PushLimited, PushRetry}
	for i, w := range want {
		if res[i].Status != w {
			t.Errorf("result %d: %v, want %v", i, res[i].Status, w)
		}
	}
	// The header covers an item without its own wait, a failure too; an item's own wait wins.
	if res[2].RetryAfter != 30*time.Second || res[3].RetryAfter != 30*time.Second || res[6].RetryAfter != 30*time.Second || res[7].RetryAfter != 5*time.Second || res[8].RetryAfter != 90*time.Second {
		t.Fatalf("retry after %v %v %v %v %v", res[2].RetryAfter, res[3].RetryAfter, res[6].RetryAfter, res[7].RetryAfter, res[8].RetryAfter)
	}
}

func TestRelaySendWholeCallFailures(t *testing.T) {
	for _, tc := range []struct {
		status    int
		body      string
		retry     string
		suspended bool
		after     time.Duration
	}{
		{http.StatusForbidden, `{"error":{"code":"suspended","message":"x"}}`, "", true, 0},
		{http.StatusForbidden, `{"error":{"code":"banned","message":"x"}}`, "", true, 0},
		{http.StatusTooManyRequests, `{"error":{"code":"rate_limited","message":"x"}}`, "120", false, 2 * time.Minute},
		{http.StatusTooManyRequests, `{"error":{"code":"rate_limited","message":"x"}}`, "", false, 0},
		{http.StatusBadGateway, `oops`, "", false, 0},
		{http.StatusUnauthorized, `{"error":{"code":"unauthorized","message":"x"}}`, "", false, 0},
	} {
		r := relayServer(t, func(w http.ResponseWriter, _ *http.Request) {
			if tc.retry != "" {
				w.Header().Set("Retry-After", tc.retry)
			}
			w.WriteHeader(tc.status)
			_, _ = io.WriteString(w, tc.body)
		})
		_, err := r.Send(context.Background(), []PushMessage{{Handle: "h_secret_handle", Ciphertext: []byte{1}}})
		var pe *PushError
		if !errors.As(err, &pe) || pe.Status != tc.status || pe.Suspended() != tc.suspended || pe.RetryAfter != tc.after ||
			pe.Limited() != (tc.status == http.StatusTooManyRequests) {
			t.Fatalf("%d: %v", tc.status, err)
		}
		if strings.Contains(err.Error(), "s3cret") || strings.Contains(err.Error(), "h_secret_handle") {
			t.Fatalf("error leaks: %v", err)
		}
	}
	// A reply with the wrong number of results is a failure of the whole call.
	r := relayServer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{"results":[]}`) })
	if _, err := r.Send(context.Background(), []PushMessage{{Handle: "h_x", Ciphertext: []byte{1}}}); err == nil {
		t.Fatal("short reply accepted")
	}
	// Unreachable.
	dead := NewRelay("http://127.0.0.1:1", "inst_abc", "s3cret", nil)
	if _, err := dead.Send(context.Background(), []PushMessage{{Handle: "h_x", Ciphertext: []byte{1}}}); err == nil || strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("%v", err)
	}
	if _, err := dead.Send(context.Background(), make([]PushMessage, MaxPushBatch+1)); err == nil {
		t.Fatal("an oversized batch was sent")
	}
}

func TestRelayHello(t *testing.T) {
	r := relayServer(t, func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/v1/hello" || req.Header.Get("Authorization") != "Bearer inst_abc.s3cret" {
			t.Errorf("%s %q", req.URL.Path, req.Header.Get("Authorization"))
		}
		_, _ = io.WriteString(w, `{"pushId":"inst_abc","name":"Ottawa Mesh","status":"suspended","tier":"email",
			"limits":{"perMinute":30,"perDay":500,"perDevicePerDay":200},"url":"https://b.example.org","domainVerified":false,"termsOutdated":true,"devices":12}`)
	})
	h, err := r.Hello(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if h.Name != "Ottawa Mesh" || h.Status != "suspended" || h.Tier != "email" || h.Limits.PerMinute != 30 || h.Limits.PerDay != 500 ||
		h.Limits.PerDevicePerDay != 200 || !h.TermsOutdated || h.Devices != 12 {
		t.Fatalf("%+v", h)
	}
	banned := relayServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"error":{"code":"banned","message":"x"}}`)
	})
	var pe *PushError
	if _, err := banned.Hello(context.Background()); !errors.As(err, &pe) || pe.Code != "banned" {
		t.Fatalf("%v", err)
	}
}

func TestRetryAfterParsing(t *testing.T) {
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	for in, want := range map[string]time.Duration{"": 0, "15": 15 * time.Second, "-3": 0, "junk": 0,
		now.Add(90 * time.Second).Format(http.TimeFormat): 90 * time.Second, "999999999": maxRetryAfter} {
		if got := retryAfter(in, now); got != want {
			t.Errorf("%q: %v, want %v", in, got, want)
		}
	}
}

// A rate limit with no wait in the item or the header still waits a minute.
func TestRelayRateLimitWithoutAWait(t *testing.T) {
	r := relayServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"results":[{"status":"rate_limited"}]}`)
	})
	res, err := r.Send(context.Background(), []PushMessage{{Handle: "h_x", Ciphertext: []byte{1}}})
	if err != nil || res[0].Status != PushLimited || res[0].RetryAfter != time.Minute {
		t.Fatalf("%+v %v", res, err)
	}
}

// A 200 whose body is cut short or unreadable is an answer we do not have: the outcome is unknown, as for a timeout.
func TestRelayIncompleteAnswerIsUnknown(t *testing.T) {
	for name, h := range map[string]http.HandlerFunc{
		"cut json": func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{"results":[{"status":"ok"}`) },
		"short body": func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "500")
			_, _ = io.WriteString(w, `{"results":[{"status":"ok"}]}`)
		},
		"wrong count": func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{"results":[]}`) },
	} {
		r := relayServer(t, h)
		_, err := r.Send(context.Background(), []PushMessage{{Handle: "h_x", Ciphertext: []byte{1}}})
		var pe *PushError
		if !errors.As(err, &pe) || pe.Status != 0 {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// Through the sender: a cut 200 retries only the batch it carried; the rest stays pending with no attempt.
func TestPushIncompleteAnswerRetriesOnlyItsBatch(t *testing.T) {
	r := relayServer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{"results":[{"status":"o`) })
	f := &fakeSend{devices: map[uuid.UUID][]Device{userA: {dev("ios", "h_a")}}}
	for i := 1; i <= 150; i++ {
		f.rows = append(f.rows, row(int64(i), userA, passOne, KindRebooted, "A", 1))
	}
	if err := pushSender(f, r).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := f.settled; len(st.PushRetry) != MaxPushBatch || len(st.PushHold) != 0 || len(st.PushDone) != 0 {
		t.Fatalf("retry %d hold %d done %d", len(st.PushRetry), len(st.PushHold), len(st.PushDone))
	}
}
