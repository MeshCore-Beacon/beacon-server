// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	// MaxPushBatch is the most messages one send call carries. The push server answers within about 20 seconds
	// whatever the size: what it did not start in time comes back rate_limited.
	MaxPushBatch = 100
	// PushTTL is how long the push server keeps trying a notification.
	PushTTL       = 24 * time.Hour
	HelloInterval = time.Hour
	pushCallLimit = 15 * time.Second
	// sendCallLimit bounds one /v1/send call, with room over the push server's own answer time.
	sendCallLimit = 30 * time.Second
	// defaultLimitWait holds a rate-limited push that came with no wait.
	defaultLimitWait = time.Minute
	maxPushReply     = 64 << 10
	maxRetryAfter    = 6 * time.Hour
)

// PushMessage is one sealed notice for one device handle. ID is the same on every retry of the notice for the
// device, so the push server can answer a repeat without sending it again.
type PushMessage struct {
	Handle      string
	Ciphertext  []byte
	CollapseKey string
	ID          string
}

// PushStatus is what became of one message.
type PushStatus int

const (
	PushDelivered PushStatus = iota // handed on: the device is reached
	PushRetry                       // try again later
	PushGone                        // the handle is dead: delete the device
	PushDropped                     // the message itself was refused: never resend it
	PushLimited                     // not sent because of a limit: wait RetryAfter, not a failed attempt
)

// PushResult is the outcome of one message; RetryAfter is set when the server asked to wait.
type PushResult struct {
	Status     PushStatus
	RetryAfter time.Duration
}

// PushSender sends up to MaxPushBatch messages in one call, answering one result per message in order. An error means
// the whole call failed and every message should be retried.
type PushSender interface {
	Send(ctx context.Context, msgs []PushMessage) ([]PushResult, error)
}

// PushError is a call the push server refused as a whole, or could not be reached for (Status 0). It never carries
// the secret, a handle or a payload.
type PushError struct {
	Status     int
	Code       string
	RetryAfter time.Duration
	msg        string
}

func (e *PushError) Error() string { return e.msg }

// Limited is true when the whole call was refused for a rate limit: hold the push, it is not a failed attempt.
func (e *PushError) Limited() bool { return e.Status == http.StatusTooManyRequests }

// Suspended is true when the push server has suspended or banned this server.
func (e *PushError) Suspended() bool { return e.Code == "suspended" || e.Code == "banned" }

// Relay is the client for the push server: POST /v1/hello and /v1/send, authenticated with the Push ID and secret.
type Relay struct {
	base, pushID, secret string
	client               *http.Client
	now                  func() time.Time
}

// NewRelay builds the client. base is the push server's address with no trailing slash; client nil uses a default
// that never follows redirects.
func NewRelay(base, pushID, secret string, client *http.Client) *Relay {
	if client == nil {
		client = &http.Client{Timeout: sendCallLimit, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	return &Relay{base: strings.TrimRight(base, "/"), pushID: pushID, secret: secret, client: client, now: time.Now}
}

// PushID is this server's Push ID.
func (r *Relay) PushID() string { return r.pushID }

func (r *Relay) post(ctx context.Context, path string, limit time.Duration, body any) (int, http.Header, []byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return 0, nil, nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.base+path, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, nil, &PushError{msg: "push server request could not be built"}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.pushID+"."+r.secret)
	resp, err := r.client.Do(req)
	if err != nil {
		// The transport error names only the address, never the header. A timeout lands here too: Status 0 means the
		// outcome is unknown, and some of the batch may have gone out.
		return 0, nil, nil, &PushError{msg: "push server unreachable"}
	}
	defer resp.Body.Close()
	reply, err := io.ReadAll(io.LimitReader(resp.Body, maxPushReply))
	if err != nil && resp.StatusCode == http.StatusOK {
		return 0, nil, nil, errIncomplete
	}
	return resp.StatusCode, resp.Header, reply, nil
}

// errIncomplete is a 200 we could not read in full: like a timeout, the outcome is unknown (Status 0).
var errIncomplete = &PushError{msg: "push server answer incomplete"}

func (r *Relay) refused(status int, h http.Header, reply []byte) *PushError {
	var e struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(reply, &e)
	code := e.Error.Code
	if len(code) > 40 {
		code = code[:40]
	}
	msg := fmt.Sprintf("push server answered %d", status)
	if code != "" {
		msg += " " + code
	}
	return &PushError{Status: status, Code: code, RetryAfter: retryAfter(h.Get("Retry-After"), r.now()), msg: msg}
}

type wireMessage struct {
	Handle      string `json:"handle"`
	Ciphertext  string `json:"ciphertext"`
	CollapseKey string `json:"collapseKey,omitempty"`
	TTLSeconds  int    `json:"ttlSeconds"`
	ID          string `json:"id,omitempty"`
}

// Send posts one batch to /v1/send.
func (r *Relay) Send(ctx context.Context, msgs []PushMessage) ([]PushResult, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	if len(msgs) > MaxPushBatch {
		return nil, &PushError{msg: "push batch too large"}
	}
	wire := make([]wireMessage, len(msgs))
	for i, m := range msgs {
		wire[i] = wireMessage{Handle: m.Handle, Ciphertext: base64.StdEncoding.EncodeToString(m.Ciphertext), CollapseKey: m.CollapseKey,
			TTLSeconds: int(PushTTL / time.Second), ID: m.ID}
	}
	status, h, reply, err := r.post(ctx, "/v1/send", sendCallLimit, map[string]any{"messages": wire})
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, r.refused(status, h, reply)
	}
	var out struct {
		Results []struct {
			Status            string `json:"status"`
			RetryAfterSeconds int64  `json:"retryAfterSeconds"`
		} `json:"results"`
	}
	if json.Unmarshal(reply, &out) != nil || len(out.Results) != len(msgs) {
		return nil, errIncomplete
	}
	wait := retryAfter(h.Get("Retry-After"), r.now())
	res := make([]PushResult, len(msgs))
	for i, x := range out.Results {
		// A wait given for the item beats the header, which carries the longest in the batch.
		itemWait := wait
		if x.RetryAfterSeconds > 0 {
			itemWait = min(time.Duration(x.RetryAfterSeconds)*time.Second, maxRetryAfter)
		}
		switch x.Status {
		case "ok":
			res[i] = PushResult{Status: PushDelivered}
		case "invalid_handle":
			res[i] = PushResult{Status: PushGone}
		case "too_large", "invalid":
			res[i] = PushResult{Status: PushDropped}
		case "rate_limited":
			if itemWait <= 0 {
				itemWait = defaultLimitWait
			}
			res[i] = PushResult{Status: PushLimited, RetryAfter: itemWait}
		default: // failed, and anything newer than this server
			res[i] = PushResult{Status: PushRetry, RetryAfter: itemWait}
		}
	}
	return res, nil
}

// HelloLimits are this server's quotas at the push server.
type HelloLimits struct {
	PerMinute       int `json:"perMinute"`
	PerDay          int `json:"perDay"`
	PerDevicePerDay int `json:"perDevicePerDay"`
}

// Hello is the push server's view of this server.
type Hello struct {
	PushID         string      `json:"pushId"`
	Name           string      `json:"name"`
	Status         string      `json:"status"`
	Tier           string      `json:"tier"`
	Limits         HelloLimits `json:"limits"`
	URL            string      `json:"url"`
	DomainVerified bool        `json:"domainVerified"`
	TermsOutdated  bool        `json:"termsOutdated"`
	Devices        int         `json:"devices"`
}

// Hello posts to /v1/hello.
func (r *Relay) Hello(ctx context.Context) (Hello, error) {
	status, h, reply, err := r.post(ctx, "/v1/hello", pushCallLimit, map[string]any{})
	if err != nil {
		return Hello{}, err
	}
	if status != http.StatusOK {
		return Hello{}, r.refused(status, h, reply)
	}
	var out Hello
	if json.Unmarshal(reply, &out) != nil {
		return Hello{}, &PushError{Status: status, msg: "push server answered with an unreadable reply"}
	}
	return out, nil
}

// RunHello says hello at once and then every HelloInterval until ctx ends, logging what the push server reports. A
// failed hello stops nothing: the outbox keeps retrying.
func RunHello(ctx context.Context, r *Relay) {
	t := time.NewTicker(HelloInterval)
	defer t.Stop()
	for {
		LogHello(ctx, r)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// LogHello says hello once and logs the answer.
func LogHello(ctx context.Context, r *Relay) {
	h, err := r.Hello(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		var pe *PushError
		if errors.As(err, &pe) && pe.Code == "banned" {
			slog.Error("this server is banned from the push server: alert push notifications cannot be sent", "component", "alerts")
			return
		}
		slog.Error("push server hello failed: alert push notifications will be retried", "component", "alerts", "error", err)
		return
	}
	attrs := []any{"component", "alerts", "status", h.Status, "tier", h.Tier, "per_minute", h.Limits.PerMinute,
		"per_day", h.Limits.PerDay, "per_device_per_day", h.Limits.PerDevicePerDay, "devices", h.Devices, "domain_verified", h.DomainVerified}
	if h.Status != "active" {
		slog.Error("push server reports this server is "+h.Status+": alert push notifications cannot be sent", attrs...)
	} else {
		slog.Info("push server hello", attrs...)
	}
	if h.TermsOutdated {
		slog.Warn("the push server terms have changed: review and accept them where you registered this server", "component", "alerts")
	}
}

// retryAfter reads Retry-After as seconds or an HTTP date, capped; 0 when absent or unreadable.
func retryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	var d time.Duration
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		if n <= 0 {
			return 0
		}
		if n > int64(maxRetryAfter/time.Second) {
			return maxRetryAfter
		}
		d = time.Duration(n) * time.Second
	} else if t, err := http.ParseTime(v); err == nil {
		d = t.Sub(now)
	}
	if d < 0 {
		return 0
	}
	return min(d, maxRetryAfter)
}
