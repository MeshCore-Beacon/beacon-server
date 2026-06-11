// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	_ "github.com/MeshCore-Beacon/beacon-server/docs"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/router"
	"github.com/MeshCore-Beacon/beacon-server/internal/cache"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/MeshCore-Beacon/beacon-server/internal/iatadb"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

//	@title			MeshCore Beacon API
//	@version		1.3.0
//	@description	MeshCore network observation backend. Ingests LoRa packets from MQTT brokers, stores in PostgreSQL, and streams live events via WebSocket.
//	@termsOfService	https://github.com/MeshCore-Beacon/beacon-server

//	@contact.name	MeshCore Beacon
//	@contact.url	https://github.com/MeshCore-Beacon/beacon-server

//	@license.name	AGPL-3-or-later

//	@host		localhost:8080
//	@BasePath	/api/v1

//	@schemes	http https

// @tag.name			IATAs
// @tag.description	Airport/location codes that group observers and packets
// @tag.name			Regions
// @tag.description	Super-regions grouping multiple IATAs
// @tag.name			Observers
// @tag.description	MeshCore MQTT observers (gateways)
// @tag.name			Nodes
// @tag.description	MeshCore radio nodes
// @tag.name			Packets
// @tag.description	LoRa packets heard by observers
// @tag.name			Channels
// @tag.description	MeshCore group text channels
// @tag.name			Messages
// @tag.description	Decrypted channel messages
// @tag.name			Brokers
// @tag.description	MQTT broker connection status
// @tag.name			Stats
// @tag.description	Network statistics and time series
func main() {
	_ = godotenv.Load()
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.yaml"
	}

	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	telemetryResolution := cfg.Telemetry.Resolution.Duration
	if telemetryResolution == 0 {
		telemetryResolution = time.Hour
	}
	telemetryRetention := cfg.Telemetry.Retention.Duration
	if telemetryRetention == 0 {
		telemetryRetention = 28 * 24 * time.Hour // 4 weeks
	}
	packetRetention := cfg.Packets.Retention.Duration
	if packetRetention == 0 {
		packetRetention = 30 * 24 * time.Hour // 30 days
	}

	maxConnsPerIP := cfg.WebSocket.MaxConnectionsPerIP
	if maxConnsPerIP == 0 {
		maxConnsPerIP = 5
	}

	// ── Hub ──────────────────────────────────────────────────────────────────
	h := hub.New()
	go h.Run()

	// ── MQTT ingest workers ──────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := pgxpool.New(ctx, mustEnv("POSTGRES_DSN"))
	if err != nil {
		log.Fatalf("failed to connect to postgres at %s: %v", os.Getenv("POSTGRES_DSN_HOST"), err)
	}
	defer pool.Close()

	if err := db.RunMigrations(ctx, pool); err != nil {
		log.Fatalf("migrations failed: %v", err)
	}

	store := db.New(pool)

	// ── Redis cache layer ────────────────────────────────────────────────────
	var reader api.Reader = store
	if redisAddr := os.Getenv("REDIS_ADDR"); redisAddr != "" {
		redisClient := cache.NewClient(
			redisAddr,
			os.Getenv("REDIS_PASSWORD"),
			func() int {
				if db := os.Getenv("REDIS_DB"); db != "" {
					n, err := strconv.Atoi(db)
					if err == nil {
						return n
					}
				}
				return 0
			}(),
		)
		if err := redisClient.Ping(ctx); err != nil {
			log.Printf("warning: redis unavailable at %s, caching disabled: %v", redisAddr, err)
		} else {
			ttls := cache.ResolveTTLs(cfg.Cache)
			reader = cache.NewCachedReader(store, redisClient, ttls)
			defer redisClient.Close()
			log.Printf("cache: Redis connected at %s (stats=%s reference=%s nodes=%s observers=%s)",
				redisAddr, ttls.Stats, ttls.Reference, ttls.Nodes, ttls.Observers)
		}
	}

	// refresh meterialized views on boot or restart to stay fresh
	refreshMaterializedViews(ctx, store)

	// ── Seed config data ─────────────────────────────────────────────────────
	if err := config.Seed(ctx, cfg, store); err != nil {
		log.Fatalf("failed to seed config: %v", err)
	}

	// ── Build transport scope keystore ───────────────────────────────────────
	scopes := scopestore.New()
	scopeEntries, err := store.GetTransportScopes(ctx)
	if err != nil {
		log.Fatalf("failed to load transport scopes: %v", err)
	}
	scopes.Load(scopeEntries)
	log.Printf("loaded %d transport scopes", len(scopeEntries))

	// ── Build channel keystore ──────────────────────────────────────────────
	entries := make(map[string][]keystore.Entry)

	// Hashtag-derived channels: secret = SHA256("#tag")[:16], hash = SHA256(secret)[0]
	for _, tag := range cfg.ChannelKeys.Hashtags {
		secret, channelHash, fingerprint := keystore.DeriveHashtagKey(tag)
		hashHex := fmt.Sprintf("%02x", channelHash)
		entry := keystore.Entry{
			Key:         secret,
			Fingerprint: fingerprint,
			Hashtag:     tag,
			Name:        "#" + tag,
		}
		if !entryExists(entries[hashHex], entry) {
			entries[hashHex] = append(entries[hashHex], entry)
			log.Printf("config: loaded hashtag channel #%s (hash=%s)", tag, hashHex)
		}
	}

	// Explicit keys: hash provided directly, key is hex-encoded
	for hashHex, keyCfg := range cfg.ChannelKeys.Keys {
		key, err := hex.DecodeString(keyCfg.Key)
		if err != nil {
			log.Printf("warning: invalid channel key for hash %s, skipping: %v", hashHex, err)
			continue
		}
		entry := keystore.Entry{
			Key:         key,
			Fingerprint: keystore.Fingerprint(key),
			Name:        keyCfg.Name,
		}
		if !entryExists(entries[hashHex], entry) {
			entries[hashHex] = append(entries[hashHex], entry)
			log.Printf("config: loaded explicit channel key for hash %s name=%q", hashHex, keyCfg.Name)
		}
	}

	keys := keystore.NewMapKeyStore(entries)

	// ── Build geographic ingest filter ───────────────────────────────────────────────────────────
	allowedIATAs := iatadb.BuildAllowedSet(cfg.Ingest.AllowCountries, cfg.Ingest.AllowContinents)
	if allowedIATAs != nil {
		log.Printf("config: ingest filter active — %d allowed IATAs (countries=%v continents=%v)",
			len(allowedIATAs), cfg.Ingest.AllowCountries, cfg.Ingest.AllowContinents)
	} else {
		log.Printf("config: ingest filter inactive — accepting all IATAs")
	}

	broker1 := ingest.New(
		ingest.Config{
			BrokerName:          "mqtt1",
			URL:                 mustEnv("MQTT_BROKER_1_URL"),
			Username:            mustEnv("MQTT_BROKER_1_USERNAME"),
			Password:            mustEnv("MQTT_BROKER_1_PASSWORD"),
			TelemetryResolution: telemetryResolution,
			AllowedIATAs:        allowedIATAs,
		},
		store,
		h,
		keys,
		scopes,
	)

	broker2 := ingest.New(
		ingest.Config{
			BrokerName:          "mqtt2",
			URL:                 mustEnv("MQTT_BROKER_2_URL"),
			Username:            mustEnv("MQTT_BROKER_2_USERNAME"),
			Password:            mustEnv("MQTT_BROKER_2_PASSWORD"),
			TelemetryResolution: telemetryResolution,
			AllowedIATAs:        allowedIATAs,
		},
		store,
		h,
		keys,
		scopes,
	)

	if cr, ok := reader.(*cache.CachedReader); ok {
		broker1.SetCacheInvalidators(cr.InvalidateNode, cr.InvalidateObserver)
		broker2.SetCacheInvalidators(cr.InvalidateNode, cr.InvalidateObserver)
	}

	go broker1.Start(ctx)
	go broker2.Start(ctx)

	// ── cleanup and materialized view refresh goroutine ─────────────────────────────────────────
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := store.DeleteOldTelemetry(ctx, time.Now().Add(-telemetryRetention)); err != nil {
					log.Printf("cleanup: delete old telemetry failed: %v", err)
				}
				if err := store.DeleteOldPackets(ctx, time.Now().Add(-packetRetention)); err != nil {
					log.Printf("cleanup: delete old packets failed: %v", err)
				}
				refreshMaterializedViews(ctx, store)
			case <-ctx.Done():
				return
			}
		}
	}()

	// ── HTTP server ──────────────────────────────────────────────────────────
	r := router.New(h, reader, []*ingest.Worker{broker1, broker2}, maxConnsPerIP, cfg.CORS)

	srv := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	go func() {
		fmt.Printf("Beacon listening on %s\n", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// ── Graceful shutdown ────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("shutting down...")
	cancel() // stops ingest workers
	if err := srv.Shutdown(context.Background()); err != nil {
		log.Printf("server shutdown error: %v", err)
	}
}

// entryExists checks if an identical key entry already exists in the slice
// to prevent loading duplicate key/hash pairs from config.
func entryExists(entries []keystore.Entry, e keystore.Entry) bool {
	for _, existing := range entries {
		if bytes.Equal(existing.Key, e.Key) {
			return true
		}
	}
	return false
}

// mustEnv returns the value of an env var and logs a warning if it is unset.
// Callers that require the value to be non-empty should fatal themselves;
// ingest workers tolerate missing broker config and will fail on connect instead.
func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Printf("warning: %s is not set", key)
	}
	return v
}

func refreshMaterializedViews(ctx context.Context, store *db.Store) {
	if err := store.RefreshHourlyStats(ctx); err != nil {
		log.Printf("refresh: materialized view for hourly stats failed: %v", err)
	}
	if err := store.RefreshTopNodes(ctx); err != nil {
		log.Printf("refresh: materialized view for top nodes failed: %v", err)
	}
	if err := store.RefreshRadioPresets(ctx); err != nil {
		log.Printf("refresh: materialized view for radio presets failed: %v", err)
	}
}
