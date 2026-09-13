// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
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
	"github.com/MeshCore-Beacon/beacon-server/internal/background"
	"github.com/MeshCore-Beacon/beacon-server/internal/cache"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/MeshCore-Beacon/beacon-server/internal/iatadb"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
	"github.com/MeshCore-Beacon/beacon-server/internal/logging"
	"github.com/MeshCore-Beacon/beacon-server/internal/presence"
	"github.com/MeshCore-Beacon/beacon-server/internal/scopestore"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

// version is set at build time via -ldflags "-X main.version=vX.Y.Z".
// Falls back to "dev" when built without the flag.
var version = "dev"

//	@title			MeshCore Beacon API
//	@version		1.6.0
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
		slog.Error("failed to load config", "component", "startup", "error", err)
		os.Exit(1)
	}
	logger, err := logging.New(os.Stderr, cfg.Log)
	if err != nil {
		slog.Error("invalid logging configuration", "component", "startup", "error", err)
		os.Exit(1)
	}
	slog.SetDefault(logger)
	slog.Info("beacon starting", "component", "startup", "version", version)
	if len(cfg.Server.TrustedProxies) == 0 {
		slog.Warn("warning: server.trusted_proxies is empty; client IP headers are ignored and proxied clients share a WebSocket connection limit", "component", "startup")
	}

	resolved := config.Resolve(cfg)

	slog.Info(fmt.Sprintf("config: loaded — %s", resolved), "component", "startup")

	// ── Hub ──────────────────────────────────────────────────────────────────
	h := hub.New()
	go h.Run()

	// ── MQTT ingest workers ──────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := pgxpool.New(ctx, getEnv("POSTGRES_DSN"))
	if err != nil {
		// Parse errors can embed the complete DSN, including its password.
		slog.Error("invalid PostgreSQL connection configuration; check POSTGRES_DSN", "component", "startup")
		os.Exit(1)
	}
	defer pool.Close()

	if err := db.RunMigrations(ctx, pool); err != nil {
		slog.Error("migrations failed", "component", "startup", "error", err)
		os.Exit(1)
	}

	store := db.New(pool, resolved.ClockDriftThreshold, resolved.NodeStaleThreshold)

	// ── Presence write coalescing ────────────────────────────────────────────
	// Ingest writes go through the coalescer; reads keep using the store.
	coalescer := presence.New(store, resolved.PresenceFlushInterval, resolved.PresencePacketTTL)
	go coalescer.Run(ctx)

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
			slog.Warn(fmt.Sprintf("warning: redis unavailable at %s, caching disabled", redisAddr), "component", "startup", "error", err)
		} else {
			ttls := cache.ResolveTTLs(cfg.Cache)
			reader = cache.NewCachedReader(store, redisClient, ttls)
			defer redisClient.Close()
			slog.Info(fmt.Sprintf("cache: Redis connected at %s (stats=%s reference=%s nodes=%s observers=%s)", redisAddr, ttls.Stats, ttls.Reference, ttls.Nodes, ttls.Observers), "component", "startup")
		}
	}

	// ── Seed config data ─────────────────────────────────────────────────────
	if err := config.Seed(ctx, cfg, store); err != nil {
		slog.Error("failed to seed config", "component", "startup", "error", err)
		os.Exit(1)
	}

	// ── Build transport scope keystore ───────────────────────────────────────
	scopes := scopestore.New()
	scopeEntries, err := store.GetTransportScopes(ctx)
	if err != nil {
		slog.Error("failed to load transport scopes", "component", "startup", "error", err)
		os.Exit(1)
	}
	scopes.Load(scopeEntries)
	slog.Info(fmt.Sprintf("loaded %d transport scopes", len(scopeEntries)), "component", "startup")

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
		if !keystore.EntryExists(entries[hashHex], entry) {
			entries[hashHex] = append(entries[hashHex], entry)
			slog.Info(fmt.Sprintf("config: loaded hashtag channel #%s (hash=%s)", tag, hashHex), "component", "startup")
		}
	}

	// Explicit keys: hash provided directly, key is hex-encoded
	for hashHex, keyCfg := range cfg.ChannelKeys.Keys {
		key, err := hex.DecodeString(keyCfg.Key)
		if err != nil {
			slog.Warn(fmt.Sprintf("warning: invalid channel key for hash %s, skipping", hashHex), "component", "startup", "error", err)
			continue
		}
		entry := keystore.Entry{
			Key:         key,
			Fingerprint: keystore.Fingerprint(key),
			Name:        keyCfg.Name,
		}
		if !keystore.EntryExists(entries[hashHex], entry) {
			entries[hashHex] = append(entries[hashHex], entry)
			slog.Info(fmt.Sprintf("config: loaded explicit channel key for hash %s name=%q", hashHex, keyCfg.Name), "component", "startup")
		}
	}

	keys := keystore.NewMapKeyStore(entries)

	// ── Backfill channel messages ────────────────────────────────────────────
	// Packets whose channel key wasn't yet configured at ingest time were stored as
	// hash-only channels and never decrypted. Retry them now against the keystore we just
	// built, so adding a channel key to the config surfaces its history on the next boot
	// instead of leaving it stranded in the DB indefinitely.
	if n, err := ingest.BackfillChannelMessages(ctx, store, keys); err != nil {
		slog.Error("config: channel message backfill failed", "component", "startup", "error", err)
	} else if n > 0 {
		slog.Info(fmt.Sprintf("config: backfilled %d previously-undecrypted channel message(s)", n), "component", "startup")
	}

	// ── Build geographic ingest filter ───────────────────────────────────────────────────────────
	allowedIATAs := iatadb.BuildAllowedSet(cfg.Ingest.AllowCountries, cfg.Ingest.AllowContinents)
	if allowedIATAs != nil {
		slog.Info(fmt.Sprintf("config: ingest filter active — %d allowed IATAs (countries=%v continents=%v)", len(allowedIATAs), cfg.Ingest.AllowCountries, cfg.Ingest.AllowContinents), "component", "startup")
	} else {
		slog.Info("config: ingest filter inactive — accepting all IATAs", "component", "startup")
	}

	broker1 := ingest.New(
		ingest.Config{
			BrokerName:          "mqtt1",
			URL:                 getEnv("MQTT_BROKER_1_URL"),
			Username:            getEnv("MQTT_BROKER_1_USERNAME"),
			Password:            getEnv("MQTT_BROKER_1_PASSWORD"),
			TelemetryResolution: resolved.TelemetryResolution,
			AllowedIATAs:        allowedIATAs,
		},
		coalescer,
		h,
		keys,
		scopes,
	)

	broker2 := ingest.New(
		ingest.Config{
			BrokerName:          "mqtt2",
			URL:                 getEnv("MQTT_BROKER_2_URL"),
			Username:            getEnv("MQTT_BROKER_2_USERNAME"),
			Password:            getEnv("MQTT_BROKER_2_PASSWORD"),
			TelemetryResolution: resolved.TelemetryResolution,
			AllowedIATAs:        allowedIATAs,
		},
		coalescer,
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

	tasks := []background.Task{
		background.ViewRefreshTask(store, resolved.ViewRefreshInterval),
		background.CleanupTask(store, resolved.TelemetryRetention, resolved.PacketRetention, resolved.NodeDeleteAfter, resolved.CleanupInterval),
		background.ReconfirmTask(store, resolved.RouteRetention, resolved.RouteGrace, int64(resolved.RouteMinObservations), resolved.ReconfirmInterval),
	}
	if resolved.ObserverDeleteAfter > 0 {
		var onDelete func(context.Context, uuid.UUID)
		if cr, ok := reader.(*cache.CachedReader); ok {
			onDelete = cr.InvalidateObserver
		}
		tasks = append(tasks, background.ObserverCleanupTask(coalescer, resolved.ObserverDeleteAfter, resolved.CleanupInterval, onDelete))
	}
	scheduler := background.New(tasks)
	go scheduler.Start(ctx)

	// ── HTTP server ──────────────────────────────────────────────────────────
	r := router.New(h, reader, []*ingest.Worker{broker1, broker2}, resolved.MaxConnsPerIP, cfg.CORS, cfg.Server)

	srv := &http.Server{
		Addr:     addr,
		Handler:  r,
		ErrorLog: slog.NewLogLogger(slog.Default().With("component", "http").Handler(), slog.LevelError),
	}

	go func() {
		slog.Info(fmt.Sprintf("Beacon listening on %s", addr), "component", "startup")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "component", "startup", "error", err)
			os.Exit(1)
		}
	}()

	// ── Graceful shutdown ────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down...", "component", "startup")
	cancel() // stops ingest workers
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown error", "component", "startup", "error", err)
	}
	coalescer.Flush(shutdownCtx)
}

// getEnv returns the value of an env var and logs a warning if it is unset.
// Callers that require the value to be non-empty should fatal themselves;
// ingest workers tolerate missing broker config and will fail on connect instead.
func getEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Warn(fmt.Sprintf("warning: %s is not set", key), "component", "startup")
	}
	return v
}
