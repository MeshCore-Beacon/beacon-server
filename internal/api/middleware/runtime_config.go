// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/cors"
)

var ErrInvalidOrigins = errors.New("allowed_origins must contain 1-32 ASCII HTTP(S) origins (at most 512 bytes each), or a sole *; paths, credentials, queries and control characters are not allowed")

type runtimePolicy struct {
	config  api.AdminConfig
	handler http.Handler
}

// RuntimeConfig owns one router's CORS handler and its nonsecret config view.
// Published policies are immutable. Updates are runtime-only, never persisted.
type RuntimeConfig struct {
	mu             sync.Mutex
	current        atomic.Pointer[runtimePolicy]
	next           http.Handler
	exposedHeaders []string
}

func NewRuntimeConfig(config api.AdminConfig, exposedHeaders []string) *RuntimeConfig {
	s := &RuntimeConfig{exposedHeaders: slices.Clone(exposedHeaders)}
	s.current.Store(&runtimePolicy{config: cloneAdminConfig(config)})
	return s
}

// CORS is bound once as the outer middleware of its owning router.
func (s *RuntimeConfig) CORS(next http.Handler) http.Handler {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next != nil {
		panic("runtime CORS already bound")
	}
	s.next = next
	s.current.Store(s.policy(s.current.Load().config))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.current.Load().handler.ServeHTTP(w, r)
	})
}

func (s *RuntimeConfig) Snapshot() api.AdminConfig {
	return cloneAdminConfig(s.current.Load().config)
}

func (s *RuntimeConfig) UpdateOrigins(origins []string) (api.AdminConfig, error) {
	validated, err := validateOrigins(origins)
	if err != nil {
		return api.AdminConfig{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	config := cloneAdminConfig(s.current.Load().config)
	config.CORS.AllowedOrigins = validated
	s.current.Store(s.policy(config))
	return cloneAdminConfig(config), nil
}

func (s *RuntimeConfig) policy(config api.AdminConfig) *runtimePolicy {
	p := &runtimePolicy{config: config}
	if s.next != nil {
		p.handler = cors.Handler(cors.Options{
			AllowedOrigins:   slices.Clone(config.CORS.AllowedOrigins),
			AllowedMethods:   slices.Clone(config.CORS.AllowedMethods),
			AllowedHeaders:   slices.Clone(config.CORS.AllowedHeaders),
			AllowCredentials: config.CORS.AllowCredentials, MaxAge: config.CORS.MaxAge,
			ExposedHeaders: slices.Clone(s.exposedHeaders),
		})(s.next)
	}
	return p
}

func cloneAdminConfig(config api.AdminConfig) api.AdminConfig {
	config.CORS.AllowedOrigins = slices.Clone(config.CORS.AllowedOrigins)
	config.CORS.AllowedMethods = slices.Clone(config.CORS.AllowedMethods)
	config.CORS.AllowedHeaders = slices.Clone(config.CORS.AllowedHeaders)
	return config
}

func validateOrigins(origins []string) ([]string, error) {
	if len(origins) == 0 || len(origins) > 32 {
		return nil, ErrInvalidOrigins
	}
	result := make([]string, len(origins))
	for i, origin := range origins {
		if len(origin) > 512 || strings.IndexFunc(origin, unicode.IsControl) >= 0 {
			return nil, ErrInvalidOrigins
		}
		origin = strings.TrimSpace(origin)
		if strings.IndexFunc(origin, func(r rune) bool { return r > unicode.MaxASCII }) >= 0 {
			return nil, ErrInvalidOrigins
		}
		origin = strings.ToLower(origin)
		if origin == "*" {
			if len(origins) != 1 {
				return nil, ErrInvalidOrigins
			}
			result[i] = origin
			continue
		}
		u, err := url.Parse(origin)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(origin, "#") || strings.Count(u.Host, "*") > 1 || strings.Contains(u.Host, "%") || strings.HasSuffix(u.Host, ":") {
			return nil, ErrInvalidOrigins
		}
		if port := u.Port(); port != "" {
			n, err := strconv.Atoi(port)
			if err != nil || n < 1 || n > 65535 {
				return nil, ErrInvalidOrigins
			}
		}
		result[i] = origin
	}
	return result, nil
}
