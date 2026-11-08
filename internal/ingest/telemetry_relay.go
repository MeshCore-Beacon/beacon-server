// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

type relayInterests struct {
	prefixes map[uint16]bool
	until    time.Time
}
type relayHearing struct {
	ObserverKey string `json:"observerKey"`
	ReceivedAt  int64  `json:"receivedAt"`
	Raw         string `json:"raw"`
}

// TelemetryRelay copies only responses for active expected requests. It adds no
// MQTT connection, RF request, database work or blocking I/O to packet ingestion.
type TelemetryRelay struct {
	endpoint, token string
	client          *http.Client
	interests       atomic.Pointer[relayInterests]
	queue           chan relayHearing
	dropped         atomic.Uint64
}

func NewTelemetryRelay(endpoint, token string) (*TelemetryRelay, error) {
	target, err := config.ParseNodeTelemetryURL(endpoint)
	if err != nil || target == nil || !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(token) {
		return nil, errors.New("observer reply relay requires the intake URL and a 32-byte private relay token")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxConnsPerHost = 2
	return &TelemetryRelay{endpoint: strings.TrimRight(endpoint, "/"), token: token, client: &http.Client{Transport: transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, queue: make(chan relayHearing, 128)}, nil
}

func (s *TelemetryRelay) Observe(observer string, raw, payload []byte) {
	interest := s.interests.Load()
	if interest == nil || !time.Now().Before(interest.until) || len(payload) < 4 || len(raw) > 512 || len(observer) != 64 || !interest.prefixes[uint16(payload[0])<<8|uint16(payload[1])] {
		return
	}
	h := relayHearing{ObserverKey: observer, ReceivedAt: time.Now().UnixMilli(), Raw: hex.EncodeToString(raw)}
	select {
	case s.queue <- h:
	default:
		s.dropped.Add(1)
	}
}

func (s *TelemetryRelay) request(ctx context.Context, path string, body []byte) ([]byte, error) {
	method := http.MethodGet
	if body != nil {
		method = http.MethodPost
	}
	req, err := http.NewRequestWithContext(ctx, method, s.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Content-Type", "application/json")
	response, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, errors.New("reply intake unavailable")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 8193))
	if err != nil || len(data) > 8192 {
		return nil, errors.New("invalid reply intake response")
	}
	return data, nil
}
func (s *TelemetryRelay) refresh(ctx context.Context) {
	raw, err := s.request(ctx, "/internal/interests", nil)
	if err != nil {
		s.interests.Store(nil)
		return
	}
	var body struct {
		Prefixes []string `json:"prefixes"`
		Expires  int      `json:"expiresInSeconds"`
	}
	if json.Unmarshal(raw, &body) != nil || len(body.Prefixes) > 512 || body.Expires < 1 || body.Expires > 5 {
		s.interests.Store(nil)
		return
	}
	next := &relayInterests{prefixes: map[uint16]bool{}, until: time.Now().Add(time.Duration(body.Expires) * time.Second)}
	for _, p := range body.Prefixes {
		b, e := hex.DecodeString(p)
		if e != nil || len(b) != 2 {
			s.interests.Store(nil)
			return
		}
		next.prefixes[uint16(b[0])<<8|uint16(b[1])] = true
	}
	s.interests.Store(next)
}
func (s *TelemetryRelay) flush(ctx context.Context) {
	batch := make([]relayHearing, 0, 8)
	for len(batch) < 8 {
		select {
		case h := <-s.queue:
			if time.Now().UnixMilli()-h.ReceivedAt < 10000 {
				batch = append(batch, h)
			}
		default:
			goto send
		}
	}
send:
	if len(batch) == 0 {
		return
	}
	body, _ := json.Marshal(map[string]any{"hearings": batch})
	// Best effort only: no retries can reissue RF or stall the normal packet feed.
	if _, err := s.request(ctx, "/internal/replies", body); err != nil {
		s.dropped.Add(uint64(len(batch)))
	}
}
func (s *TelemetryRelay) Run(ctx context.Context) {
	refresh := time.NewTicker(2 * time.Second)
	flush := time.NewTicker(time.Second)
	defer refresh.Stop()
	defer flush.Stop()
	defer s.client.CloseIdleConnections()
	s.refresh(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-refresh.C:
			s.refresh(ctx)
		case <-flush.C:
			s.flush(ctx)
		}
	}
}
