// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// PushMessage is one notification for one device. Data carries targetKind, publicKey and, when the radio is known,
// its id so the app can open the radio.
type PushMessage struct {
	Token, Title, Body string
	Data               map[string]string
}

// ErrUnregistered means the push service no longer knows the token: delete it.
var ErrUnregistered = errors.New("alerts: push token unregistered")

// PushSender sends one push message.
type PushSender interface {
	Send(ctx context.Context, m PushMessage) error
}

const (
	fcmScope       = "https://www.googleapis.com/auth/firebase.messaging"
	fcmSendBase    = "https://fcm.googleapis.com"
	pushCallLimit  = 10 * time.Second
	maxPushReply   = 64 << 10
	tokenLifetime  = time.Hour
	tokenRefreshIn = 2 * time.Minute // refresh this long before the access token expires
)

// FCM sends through Firebase Cloud Messaging's HTTP v1 API with an access token it obtains from a service-account
// key (a signed JWT exchanged at the key's token endpoint) and caches until shortly before it expires. Nothing it
// holds (key, access token, device tokens) is ever logged or put in an error.
type FCM struct {
	projectID, clientEmail, tokenURL, sendBase string
	key                                        *rsa.PrivateKey
	client                                     *http.Client
	now                                        func() time.Time

	mu     sync.Mutex
	access string
	expiry time.Time
}

type serviceAccount struct {
	ProjectID   string `json:"project_id"`
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
	TokenURI    string `json:"token_uri"`
}

// LoadFCM reads a Google service-account JSON file. The error never includes the file's content.
func LoadFCM(path string) (*FCM, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("push credentials file cannot be read")
	}
	return ParseFCM(raw)
}

// ParseFCM builds the sender from service-account JSON.
func ParseFCM(raw []byte) (*FCM, error) {
	var sa serviceAccount
	if json.Unmarshal(raw, &sa) != nil || sa.ProjectID == "" || sa.ClientEmail == "" || sa.PrivateKey == "" {
		return nil, errors.New("push credentials file is not a service-account key")
	}
	tokenURL := sa.TokenURI
	if tokenURL == "" {
		tokenURL = "https://oauth2.googleapis.com/token"
	}
	if u, err := url.Parse(tokenURL); err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, errors.New("push credentials token_uri must be an https URL")
	}
	block, _ := pem.Decode([]byte(sa.PrivateKey))
	if block == nil {
		return nil, errors.New("push credentials private key is not PEM")
	}
	var key *rsa.PrivateKey
	if k, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
		rk, ok := k.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("push credentials private key is not RSA")
		}
		key = rk
	} else if k, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		key = k
	} else {
		return nil, errors.New("push credentials private key cannot be parsed")
	}
	return NewFCM(sa.ProjectID, sa.ClientEmail, tokenURL, fcmSendBase, key, nil, nil), nil
}

// NewFCM builds the sender from its parts; tests point tokenURL and sendBase at a local server.
func NewFCM(projectID, clientEmail, tokenURL, sendBase string, key *rsa.PrivateKey, client *http.Client, now func() time.Time) *FCM {
	if client == nil {
		client = &http.Client{Timeout: pushCallLimit, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	if now == nil {
		now = time.Now
	}
	return &FCM{projectID: projectID, clientEmail: clientEmail, tokenURL: tokenURL, sendBase: strings.TrimRight(sendBase, "/"), key: key, client: client, now: now}
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func (f *FCM) assertion() (string, error) {
	now := f.now()
	hdr, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT"})
	claims, _ := json.Marshal(map[string]any{"iss": f.clientEmail, "scope": fcmScope, "aud": f.tokenURL,
		"iat": now.Unix(), "exp": now.Add(tokenLifetime).Unix()})
	signing := b64(hdr) + "." + b64(claims)
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, f.key, crypto.SHA256, sum[:])
	if err != nil {
		return "", errors.New("push token request could not be signed")
	}
	return signing + "." + b64(sig), nil
}

// accessToken returns the cached access token, getting a new one when it is near expiry.
func (f *FCM) accessToken(ctx context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.access != "" && f.now().Add(tokenRefreshIn).Before(f.expiry) {
		return f.access, nil
	}
	jwt, err := f.assertion()
	if err != nil {
		return "", err
	}
	form := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"}, "assertion": {jwt}}
	ctx, cancel := context.WithTimeout(ctx, pushCallLimit)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", errors.New("push token request failed")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := f.client.Do(req)
	if err != nil {
		return "", errors.New("push token endpoint unreachable")
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxPushReply))
	var out struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if resp.StatusCode != http.StatusOK || json.Unmarshal(body, &out) != nil || out.AccessToken == "" {
		return "", fmt.Errorf("push token endpoint answered %d", resp.StatusCode)
	}
	f.access = out.AccessToken
	f.expiry = f.now().Add(time.Duration(out.ExpiresIn) * time.Second)
	return f.access, nil
}

func (f *FCM) forgetToken() {
	f.mu.Lock()
	f.access = ""
	f.mu.Unlock()
}

// Send posts one message. ErrUnregistered when Google says the device token is gone; other failures carry only the
// HTTP status.
func (f *FCM) Send(ctx context.Context, m PushMessage) error {
	for attempt := 0; ; attempt++ {
		access, err := f.accessToken(ctx)
		if err != nil {
			return err
		}
		body, _ := json.Marshal(map[string]any{"message": map[string]any{
			"token":        m.Token,
			"notification": map[string]string{"title": m.Title, "body": m.Body},
			"data":         m.Data,
		}})
		cctx, cancel := context.WithTimeout(ctx, pushCallLimit)
		req, err := http.NewRequestWithContext(cctx, http.MethodPost, f.sendBase+"/v1/projects/"+url.PathEscape(f.projectID)+"/messages:send", bytes.NewReader(body))
		if err != nil {
			cancel()
			return errors.New("push request failed")
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+access)
		resp, err := f.client.Do(req)
		if err != nil {
			cancel()
			return errors.New("push service unreachable")
		}
		reply, _ := io.ReadAll(io.LimitReader(resp.Body, maxPushReply))
		resp.Body.Close()
		cancel()
		switch {
		case resp.StatusCode == http.StatusOK:
			return nil
		case resp.StatusCode == http.StatusUnauthorized && attempt == 0:
			f.forgetToken() // the cached access token went stale: get a new one once
			continue
		case unregistered(resp.StatusCode, reply):
			return ErrUnregistered
		}
		return fmt.Errorf("push service answered %d", resp.StatusCode)
	}
}

// unregistered reads Google's error: 404 NOT_FOUND or UNREGISTERED, or 400 with an INVALID_ARGUMENT about the token.
func unregistered(status int, reply []byte) bool {
	if status != http.StatusNotFound && status != http.StatusBadRequest {
		return false
	}
	var e struct {
		Error struct {
			Status  string `json:"status"`
			Details []struct {
				ErrorCode string `json:"errorCode"`
			} `json:"details"`
		} `json:"error"`
	}
	if json.Unmarshal(reply, &e) != nil {
		return false
	}
	for _, d := range e.Error.Details {
		if d.ErrorCode == "UNREGISTERED" {
			return true
		}
	}
	return status == http.StatusNotFound && (e.Error.Status == "NOT_FOUND" || e.Error.Status == "UNREGISTERED")
}
