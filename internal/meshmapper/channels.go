// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package meshmapper

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/iatadb"
	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
)

const (
	MaxChannels    = 256 // per region
	MaxChannelBody = 64 << 10

	channelRetry = config.MinChannelsRefresh // get_channels.php allows one request per region per 23.5h, failed or not
)

type ChannelStore interface {
	ListKnownIATAs(ctx context.Context) ([]string, error)
	ListChannelCatalogues(ctx context.Context) ([]Catalogue, error)
	// SaveChannelCatalogue replaces the IATA's members when the cache carries a new payload.
	SaveChannelCatalogue(ctx context.Context, iata, url string, cache Cache, fingerprints [][]byte) error
	ClearChannelMembers(ctx context.Context) error
}

// ChannelKeys receives every imported key and reports the channel hashes it hadn't seen.
type ChannelKeys interface {
	SetImported(entries []keystore.Entry) [][]byte
}

type channelSource struct {
	iata, country, url string
	cache              Cache
	entries            []keystore.Entry
}

// Channels is owned by one background task.
type Channels struct {
	store       ChannelStore
	keys        ChannelKeys
	dir         *Directory
	channelsURL func(site string) (string, bool)
	client      *http.Client
	interval    time.Duration
	seen        map[string]bool
	sources     []channelSource
	onNewKeys   func(hashes [][]byte)
}

// NewChannels restores saved channel lists into keys without HTTP requests.
// Disabled, it clears MeshMapper's region membership so config scoping alone applies.
func NewChannels(ctx context.Context, cfg config.MeshMapperChannelsConfig, store ChannelStore, dir *Directory, keys ChannelKeys) (*Channels, error) {
	c := &Channels{store: store, keys: keys, dir: dir, channelsURL: channelsEndpoint, interval: cfg.Interval(), seen: map[string]bool{},
		client: &http.Client{
			Timeout:       requestTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}}
	if !cfg.Enabled {
		if err := store.ClearChannelMembers(ctx); err != nil {
			return nil, fmt.Errorf("clear MeshMapper channel members: %w", err)
		}
		return c, nil
	}
	saved, err := store.ListChannelCatalogues(ctx)
	if err != nil {
		return nil, fmt.Errorf("restore channel catalogues: %w", err)
	}
	latest := map[string]Catalogue{}
	for _, cat := range saved {
		if old, ok := latest[cat.IATA]; !ok || cat.AttemptedAt.After(old.AttemptedAt) {
			latest[cat.IATA] = cat
		}
	}
	iatas, err := store.ListKnownIATAs(ctx)
	if err != nil {
		return nil, fmt.Errorf("list IATAs for MeshMapper channels: %w", err)
	}
	c.track(iatas, latest)
	c.publish()
	return c, nil
}

// OnNewKeys is told the channel hashes an import added, for backfill.
func (c *Channels) OnNewKeys(fn func(hashes [][]byte)) { c.onNewKeys = fn }

func (c *Channels) track(iatas []string, saved map[string]Catalogue) {
	for _, iata := range iatas {
		if c.seen[iata] {
			continue
		}
		c.seen[iata] = true
		country := iatadb.CountryFor(iata)
		if country == "" {
			continue
		}
		s := channelSource{iata: iata, country: country}
		if cat, ok := saved[iata]; ok {
			s.url, s.cache = cat.URL, cat.Cache
			if len(cat.Payload) > 0 {
				var err error
				if s.entries, err = decodeChannels(cat.Payload, iata); err != nil {
					s.entries, s.cache.Payload, s.cache.ETag = nil, nil, ""
					s.cache.LastError = "invalid saved catalogue; awaiting refresh"
				}
			}
			c.log(s, "restored")
		}
		c.sources = append(c.sources, s)
	}
}

// Refresh makes at most one request. The scheduler serializes calls every 15s.
func (c *Channels) Refresh(ctx context.Context) (err error) {
	parent := ctx
	defer func() {
		if parent.Err() != nil {
			err = nil
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()
	now := time.Now().UTC()
	iatas, err := c.store.ListKnownIATAs(ctx)
	if err != nil {
		return fmt.Errorf("list IATAs for MeshMapper channels: %w", err)
	}
	c.track(iatas, nil)
	for n := range c.sources {
		s := &c.sources[n]
		if now.Before(s.cache.NextAttempt) {
			continue
		}
		zones, fetched, err := c.dir.List(ctx, s.country, now)
		if err != nil || fetched {
			return err
		}
		if zones == nil {
			continue
		}
		entry, listed := zones[s.iata]
		endpoint, valid := "", false
		if listed {
			endpoint, valid = c.channelsURL(entry.url)
		}
		if !valid {
			// No request to make; channels already imported stay until a list replaces them.
			s.cache.NextAttempt, s.cache.LastError = now.Add(c.interval), "not listed"
			if listed {
				s.cache.LastError = "invalid site URL"
			}
			c.log(*s, "skipped")
			continue
		}
		if endpoint != s.url {
			s.url, s.cache.ETag = endpoint, ""
		}
		return c.refresh(ctx, s, now)
	}
	return nil
}

func (c *Channels) refresh(ctx context.Context, s *channelSource, now time.Time) error {
	update := Cache{AttemptedAt: now, NextAttempt: now.Add(c.interval)}
	etag := ""
	if len(s.cache.Payload) > 0 {
		etag = s.cache.ETag
	}
	// Recorded first: an abandoned request may still have used the region's call.
	attempt := Cache{AttemptedAt: now, NextAttempt: now.Add(min(c.interval, channelRetry)), LastError: "no response"}
	s.cache.NextAttempt = attempt.NextAttempt
	if err := c.store.SaveChannelCatalogue(ctx, s.iata, s.url, attempt, nil); err != nil {
		return fmt.Errorf("persist channel catalogue %s: %w", s.iata, err)
	}
	s.cache.AttemptedAt, s.cache.LastError = now, attempt.LastError
	status, body, header, err := get(ctx, c.client, "Beacon-MeshMapper-Channels/1", s.url, etag, MaxChannelBody)
	if err != nil {
		return err
	}
	var entries []keystore.Entry
	var retryAt time.Time
	switch status {
	case http.StatusOK:
		if entries, err = decodeChannels(body, s.iata); err != nil {
			update.LastError = "invalid response"
		} else {
			update.Payload = body
		}
	case http.StatusNotModified:
		if len(s.cache.Payload) == 0 {
			update.LastError = "304 without cached catalogue"
		}
	default:
		update.LastError = statusProblem(status, header, now, &retryAt)
	}
	if update.LastError == "" {
		etag := header.Get("ETag")
		if len(etag) > 256 || strings.ContainsAny(etag, "\r\n") {
			update.LastError, update.Payload, entries = "invalid ETag", nil, nil
		} else {
			if status == http.StatusNotModified && etag == "" {
				etag = s.cache.ETag
			}
			update.ETag, update.CheckedAt = etag, now
		}
	}
	if update.LastError != "" {
		update.NextAttempt = now.Add(min(c.interval, channelRetry))
		if retryAt.After(update.NextAttempt) {
			update.NextAttempt = retryAt
		}
	}
	var fingerprints [][]byte
	for _, e := range entries {
		fingerprints = append(fingerprints, e.Fingerprint)
	}
	if update.Payload != nil && fingerprints == nil {
		fingerprints = [][]byte{}
	}
	if err := c.store.SaveChannelCatalogue(ctx, s.iata, s.url, update, fingerprints); err != nil {
		s.cache.NextAttempt = update.NextAttempt // never retry every tick during a DB outage
		return fmt.Errorf("persist channel catalogue %s: %w", s.iata, err)
	}
	if update.Payload != nil {
		s.cache.Payload, s.entries = update.Payload, entries
	}
	if !update.CheckedAt.IsZero() {
		s.cache.CheckedAt, s.cache.ETag = update.CheckedAt, update.ETag
	}
	s.cache.AttemptedAt, s.cache.NextAttempt, s.cache.LastError = now, update.NextAttempt, update.LastError
	if update.Payload != nil {
		c.publish()
	}
	c.log(*s, "checked")
	return nil
}

func (c *Channels) publish() {
	var all []keystore.Entry
	for _, s := range c.sources {
		all = append(all, s.entries...)
	}
	if added := c.keys.SetImported(all); len(added) > 0 && c.onNewKeys != nil {
		c.onNewKeys(added)
	}
}

func (c *Channels) log(s channelSource, action string) {
	level := slog.LevelInfo
	if s.cache.LastError != "" && s.cache.LastError != "not listed" {
		level = slog.LevelWarn
	}
	slog.Log(context.Background(), level, "MeshMapper channels "+action, "component", "meshmapper.channels", "iata", s.iata, "source", s.url,
		"channels", len(s.entries), "checked_at", s.cache.CheckedAt, "next_attempt", s.cache.NextAttempt, "last_error", s.cache.LastError)
}

func channelsEndpoint(site string) (string, bool) { return siteEndpoint(site, "get_channels.php") }

// decodeChannels accepts a single region's list only when every key and hash is
// exactly what Beacon derives from the name, so a bad entry can't plant a wrong key.
func decodeChannels(body []byte, iata string) ([]keystore.Entry, error) {
	var document struct {
		GeneratedAt time.Time `json:"generated_at"`
		Region      string    `json:"region"`
		Zones       []string  `json:"zones"`
		Count       *int      `json:"count"`
		Channels    *[]struct {
			Name  string   `json:"name"`
			Key   string   `json:"key"`
			Hash  string   `json:"hash"`
			Zones []string `json:"zones"`
		} `json:"channels"`
	}
	if len(body) > MaxChannelBody || !utf8.Valid(body) {
		return nil, fmt.Errorf("invalid channel list size/encoding")
	}
	if err := json.Unmarshal(body, &document); err != nil {
		return nil, err
	}
	if document.GeneratedAt.IsZero() || document.Region != iata || len(document.Zones) != 1 || document.Zones[0] != iata ||
		document.Channels == nil || document.Count == nil || *document.Count != len(*document.Channels) || len(*document.Channels) > MaxChannels {
		return nil, fmt.Errorf("invalid regional channel list")
	}
	entries := make([]keystore.Entry, 0, len(*document.Channels))
	seen := map[string]bool{}
	for _, ch := range *document.Channels {
		tag, ok := strings.CutPrefix(ch.Name, "#")
		if !ok || tag == "" || len(ch.Name) > 64 || strings.ToLower(ch.Name) != ch.Name || seen[ch.Name] ||
			strings.ContainsFunc(tag, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) || r == '#' }) ||
			len(ch.Zones) != 1 || ch.Zones[0] != iata {
			return nil, fmt.Errorf("invalid channel entry")
		}
		secret, hash, fingerprint := keystore.DeriveHashtagKey(tag)
		key, err := hex.DecodeString(ch.Key)
		if err != nil || !bytes.Equal(key, secret) || !strings.EqualFold(ch.Hash, hex.EncodeToString([]byte{hash})) {
			return nil, fmt.Errorf("channel %s key does not match its name", ch.Name)
		}
		seen[ch.Name] = true
		entries = append(entries, keystore.Entry{Key: secret, Fingerprint: fingerprint, Hashtag: tag, Name: ch.Name})
	}
	return entries, nil
}
