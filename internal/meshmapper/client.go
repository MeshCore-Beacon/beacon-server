// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshmapper

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
)

var (
	errMissingKey        = errors.New("unconfigured: MeshMapper API key is missing")
	errInvalidKey        = errors.New("unconfigured: MeshMapper API key contains invalid characters")
	errUntrustedEndpoint = errors.New("untrusted MeshMapper endpoint")
)

func newClient(apiKey string) *http.Client {
	return &http.Client{
		Timeout:       requestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Transport:     &authenticatedTransport{key: apiKey, base: http.DefaultTransport},
	}
}

type authenticatedTransport struct {
	key  string
	base http.RoundTripper
}

func (t *authenticatedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.key == "" {
		return nil, errMissingKey
	}
	if strings.ContainsFunc(t.key, func(r rune) bool { return r <= ' ' || r >= 127 }) {
		return nil, errInvalidKey
	}
	if !trustedEndpoint(req.URL) || (req.Host != "" && req.Host != req.URL.Host) {
		return nil, errUntrustedEndpoint
	}
	// Clone so credentials never enter a caller's request or redirect headers.
	request := req.Clone(req.Context())
	request.Header.Set("X-API-Key", t.key)
	return t.base.RoundTrip(request)
}

func trustedEndpoint(u *url.URL) bool {
	if u.Scheme != "https" || u.User != nil || u.Fragment != "" || u.Opaque != "" ||
		(u.Host != "meshmapper.net" && !zoneSite.MatchString(u.Host)) || u.EscapedPath() != u.Path {
		return false
	}
	switch u.Path {
	case "/get_zones.php", "/get_geojson.php", "/get_scopes.php", "/get_channels.php", "/get_repeaters.php":
		return true
	}
	return false
}

func requestProblem(err error) string {
	for _, problem := range []error{errMissingKey, errInvalidKey, errUntrustedEndpoint} {
		if errors.Is(err, problem) {
			return problem.Error()
		}
	}
	return "request failed" // Transport errors may contain URLs or credentials.
}
