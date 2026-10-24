// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	pgdata "github.com/MeshCore-Beacon/beacon-server/db/planguard"
	"github.com/MeshCore-Beacon/beacon-server/db/planpolicy"
	"github.com/MeshCore-Beacon/beacon-server/docs"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/MeshCore-Beacon/beacon-server/internal/api/router"
	"github.com/MeshCore-Beacon/beacon-server/internal/background"
	"github.com/MeshCore-Beacon/beacon-server/internal/borders"
	"github.com/MeshCore-Beacon/beacon-server/internal/cache"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/guards"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/MeshCore-Beacon/beacon-server/internal/iatadb"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
	"github.com/MeshCore-Beacon/beacon-server/internal/logging"
	"github.com/MeshCore-Beacon/beacon-server/internal/meshmapper"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
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
//	@version		2.1.0
//	@description	MeshCore network observation backend. Ingests LoRa packets from MQTT brokers, stores in PostgreSQL, and streams live events via WebSocket.
//	@description	REST requests share a configurable per-client rate limit (default 600/minute and a 600-request one-second burst cap). Exceeded limits return HTTP 429 with error.code=rate_limited and a Retry-After header in seconds. CORS preflights and WebSocket upgrades do not consume this API budget.
//	@description	WebSocket upgrade attempts at /ws have a configurable per-client limit (default 10/minute, including failed handshakes). Rate exhaustion returns HTTP 429 with Retry-After before upgrade. An accepted socket exceeding the concurrent cap closes with code 1013 before hello; established connections remain open.
//	@description	Database cost guards: a guarded family returns 429 (rate_limited, Retry-After 1) above 4 concurrent database requests per client, or 503 with Retry-After (deadline_exceeded, 5 s; service_unavailable, 2 s). Families observe by default and refuse nothing until an operator enforces them.
//	@termsOfService	https://github.com/MeshCore-Beacon/beacon-server

//	@contact.name	MeshCore Beacon
//	@contact.url	https://github.com/MeshCore-Beacon/beacon-server

//	@license.name	AGPL-3-or-later

//	@host		localhost:8080
//	@BasePath	/api/v1

//	@schemes	http https

// @securityDefinitions.apikey AdminKey
// @in header
// @name Authorization
// @description Enter Bearer followed by the configured operator key. Use HTTPS.

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
// @tag.name			Info
// @tag.description	Server info for the BEACON Mobile app
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
	resolved := config.Resolve(cfg)
	if msg := proxyLimitWarning(resolved, len(cfg.Server.TrustedProxies)); msg != "" {
		slog.Warn(msg, "component", "startup")
	}
	for _, key := range cfg.IgnoredKeys() {
		slog.Warn("config: "+key+" is no longer used and is ignored; remove it", "component", "startup")
	}
	borderFiles, err := config.LoadLocalBorders(cfg)
	if err != nil {
		slog.Error("invalid local border configuration", "component", "startup", "error", err)
		os.Exit(1)
	}
	var localBorders *borders.Live
	if cfg.Nodes.MarkForeign {
		local, _ := config.BuildLocalBorders(borderFiles, nil) // checked by LoadLocalBorders
		localBorders = borders.NewLive(local)
	}

	slog.Info(fmt.Sprintf("config: loaded — %s", resolved), "component", "startup")

	// ── Hub ──────────────────────────────────────────────────────────────────
	h := hub.New()
	go h.Run()

	// ── MQTT ingest workers ──────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dsn := getEnv("POSTGRES_DSN")
	layout := resolved.APIPlanLayout
	mode := bootModeFor(resolved.ReadOnly)
	var tune []func(string, *pgxpool.Config)
	if resolved.ReadOnly {
		slog.Warn(readOnlyWarning, "component", "startup")
		tune = append(tune, readOnlyTune)
	}
	pools, err := db.OpenPools(ctx, dsn, layout, tune...)
	if err != nil {
		// Parse errors can embed the complete DSN, including its password.
		slog.Error("invalid PostgreSQL connection configuration; check POSTGRES_DSN", "component", "startup")
		os.Exit(1)
	}
	defer pools.Close()
	slog.Info("database pools open", "component", "startup", "layout", layout, "connections", pools.Total())
	backupOpts := configureBackup(ctx, cfg, pools.Maint, dsn, configPath)

	if mode.Migrate {
		if err := db.RunMigrations(ctx, pools.Maint); err != nil {
			slog.Error("migrations failed", "component", "startup", "error", err)
			os.Exit(1)
		}
	}
	// The read-model boot repair waits for every transaction that began before this instant.
	bootTS, err := db.DatabaseNow(ctx, pools.Maint)
	if err != nil {
		slog.Error("reading the database clock failed", "component", "startup", "error", err)
		os.Exit(1)
	}
	// Trust in each read model is per process: earned by this boot's repair, dropped by any writer's failure.
	readModels := &db.ReadModels{
		Cmi:         db.NewTrust("cmi", resolved.ReadModelCmiEnabled, db.CmiTrustPolicy),
		PacketSites: db.NewTrust("packet_sites", resolved.ReadModelPacketSitesEnabled, db.PacketSitesTrustPolicy),
	}

	// Three stores: ingest writes, background and operator work, and API reads. The API store
	// routes each statement by plan class through the guards; it never runs background jobs.
	enforce, _ := guards.ParseFamilies(resolved.GuardsEnforce) // validated by config.Load
	guard := guards.New(guards.Options{Enforce: enforce, ForceObserve: layout == planpolicy.Single, Budget: resolved.GuardsBudget})
	// Ingest and maintenance share one channel id cache; the API never writes channels.
	channelIDs := db.NewChannelIDCache()
	ingestStore := db.New(pools.Ingest, pools.Ingest, resolved.ClockDriftThreshold, resolved.NodeStaleThreshold, db.WithChannelIDs(channelIDs), db.WithReadModels(readModels))
	fillPacing := db.FillPacing{Duty: resolved.FillDuty, MinPause: resolved.FillMinPause, PacketSitesSlice: resolved.FillPacketSitesSlice, CmiBatch: resolved.FillCmiBatch}
	maintStore := db.New(pools.Maint, pools.Maint, resolved.ClockDriftThreshold, resolved.NodeStaleThreshold, db.WithChannelIDs(channelIDs), db.WithReadModels(readModels), db.WithFillPacing(fillPacing))
	apiStore := db.NewAPIStore(pools, layout, guard, resolved.ClockDriftThreshold, resolved.NodeStaleThreshold, db.WithReadModels(readModels))
	if err := maintStore.PreloadChannelIDs(ctx); err != nil {
		slog.Warn("channel id preload failed; known channels upsert until cached", "component", "startup", "error", err)
	} else {
		slog.Info(fmt.Sprintf("preloaded %d channel ids", channelIDs.Len()), "component", "startup")
	}

	// ── Presence write coalescing ────────────────────────────────────────────
	// Ingest writes go through the coalescer; reads use the API store.
	coalescer := presence.New(ingestStore, resolved.PresenceFlushInterval, resolved.PresencePacketTTL)
	finishPresence := func(context.Context) {}
	if mode.Coalescer {
		finishPresence = coalescer.Start(ctx)
	}

	// ── Redis cache layer ────────────────────────────────────────────────────
	var reader api.Reader = apiStore
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
			reader = cache.NewCachedReader(apiStore, redisClient, ttls)
			defer redisClient.Close()
			slog.Info(fmt.Sprintf("cache: Redis connected at %s (stats=%s reference=%s nodes=%s observers=%s)", redisAddr, ttls.Stats, ttls.Reference, ttls.Nodes, ttls.Observers), "component", "startup")
		}
	}

	// ── Seed config data ─────────────────────────────────────────────────────
	if mode.Seed {
		if err := config.Seed(ctx, cfg, maintStore); err != nil {
			slog.Error("failed to seed config", "component", "startup", "error", err)
			os.Exit(1)
		}
	}

	// ── Build transport scope keystore ───────────────────────────────────────
	scopes := scopestore.New()
	scopeEntries, err := maintStore.GetTransportScopes(ctx)
	if err != nil {
		slog.Error("failed to load transport scopes", "component", "startup", "error", err)
		os.Exit(1)
	}
	scopes.Load(scopeEntries)
	scopes.SetManualMembers(cfg.ManualScopeMembers())
	slog.Info(fmt.Sprintf("loaded %d transport scopes", len(scopeEntries)), "component", "startup")
	directory := meshmapper.NewDirectory(maintStore, cfg.MeshMapper.APIKey)
	if mm := cfg.MeshMapper; mode.MeshMapper && (mm.Scopes.Enabled || mm.Zones.Enabled || mm.Channels.Enabled || mm.Repeaters.Enabled) {
		if cfg.MeshMapper.APIKey == "" {
			slog.Warn("MeshMapper integration unconfigured: set MESHMAPPER_API_KEY or meshmapper.api_key; retaining cached data", "component", "meshmapper")
		}
		restoreCtx, cancelRestore := context.WithTimeout(ctx, 10*time.Second)
		err = directory.Restore(restoreCtx)
		cancelRestore()
		if err != nil {
			slog.Error("failed to restore MeshMapper zone lists", "component", "startup", "error", err)
			os.Exit(1)
		}
	}
	// Read-only servers restore too: region scope lists need the catalogues, and the restore only reads.
	var scopeImporter *meshmapper.Importer
	if cfg.MeshMapper.Scopes.Enabled {
		restoreCtx, cancelRestore := context.WithTimeout(ctx, 10*time.Second)
		scopeImporter, err = meshmapper.New(restoreCtx, cfg.MeshMapper.Scopes, maintStore, directory, scopes, scopeEntries)
		cancelRestore()
		if err != nil {
			slog.Error("failed to restore MeshMapper scope catalogues", "component", "startup", "error", err)
			os.Exit(1)
		}
	}

	// Always restored, so disabling the import prunes it and file borders return.
	zones := meshmapper.NewZones(cfg.MeshMapper.Zones, maintStore, directory)
	var detailed []string
	for iata, d := range cfg.IATAs {
		if d.Name != "" || d.Lat != nil || d.Lng != nil {
			detailed = append(detailed, iata)
		}
	}
	zones.SetConfiguredIATAs(detailed)
	if cr, ok := reader.(*cache.CachedReader); ok {
		zones.OnChange(cr.InvalidateIATABorder)
		zones.OnRegionsChange(cr.InvalidateRegions)
		zones.OnIATAsChange(cr.InvalidateIATAs)
	}
	if localBorders != nil {
		zones.OnUpdate(func(imported map[string]json.RawMessage) {
			local, err := config.BuildLocalBorders(borderFiles, imported)
			if err != nil {
				slog.Error("MeshMapper boundaries rejected for foreign marking", "component", "meshmapper.zones", "error", err)
				return
			}
			localBorders.Store(local)
		})
	}
	// Restore prunes boundaries a disabled import left behind, so a read-only server skips it.
	if mode.MeshMapper {
		restoreCtx, cancelRestore := context.WithTimeout(ctx, 10*time.Second)
		err = zones.Restore(restoreCtx)
		cancelRestore()
		if err != nil {
			slog.Error("failed to restore MeshMapper boundaries", "component", "startup", "error", err)
			os.Exit(1)
		}
	}
	if localBorders != nil && !localBorders.Ready() {
		slog.Warn("nodes.mark_foreign has no boundaries yet; waiting for MeshMapper", "component", "startup")
	}

	// ── Build channel keystore ──────────────────────────────────────────────
	entries := make(map[string][]keystore.Entry)

	// Hashtag-derived channels: secret = SHA256("#tag")[:16], hash = SHA256(secret)[0]
	for _, hashtag := range cfg.ChannelKeys.Hashtags {
		tag := hashtag.Name
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

	// Restored before the boot backfill so saved MeshMapper keys decrypt history too. Both
	// constructors may clear rows when their import is disabled, so a read-only server skips them.
	var channelImporter *meshmapper.Channels
	var repeaterImporter *meshmapper.Repeaters
	if mode.MeshMapper {
		restoreCtx, cancelRestore := context.WithTimeout(ctx, 10*time.Second)
		channelImporter, err = meshmapper.NewChannels(restoreCtx, cfg.MeshMapper.Channels, maintStore, directory, keys)
		cancelRestore()
		if err != nil {
			slog.Error("failed to restore MeshMapper channels", "component", "startup", "error", err)
			os.Exit(1)
		}
		var onRepeatersChange func(context.Context)
		if cr, ok := reader.(*cache.CachedReader); ok {
			onRepeatersChange = cr.InvalidateNodeDetails
		}
		restoreCtx, cancelRestore = context.WithTimeout(ctx, 10*time.Second)
		repeaterImporter, err = meshmapper.NewRepeaters(restoreCtx, cfg.MeshMapper.Repeaters, maintStore, directory, onRepeatersChange)
		cancelRestore()
		if err != nil {
			slog.Error("failed to restore MeshMapper repeaters", "component", "startup", "error", err)
			os.Exit(1)
		}
	}
	// ── Backfill channel messages ────────────────────────────────────────────
	// Packets whose channel key wasn't yet configured at ingest time were stored as
	// hash-only channels and never decrypted. Retry them against the keystore we just
	// built, so adding a channel key to the config surfaces its history after the next boot
	// instead of leaving it stranded in the DB indefinitely. It runs in the background once
	// the server listens (reading every undecrypted packet takes a while on a slow disk);
	// imported keys queue their channels on the same job, off the refresh tick.
	var decryptBackfill *ingest.DecryptBackfill
	if mode.Backfill {
		// Paced by duty alone: most packets fail to decrypt, which costs CPU, not the database, so a fixed
		// pause per batch would only stretch the run.
		decryptBackfill = ingest.NewDecryptBackfill(maintStore, keys, db.FillPacing{Duty: resolved.FillDuty})
		if channelImporter != nil {
			channelImporter.OnNewKeys(decryptBackfill.RequestHashes)
		}
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
			LocalBorders:        localBorders,
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
			LocalBorders:        localBorders,
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
		if scopeImporter != nil {
			scopeImporter.SetCacheInvalidator(cr.InvalidateScopeNames)
		}
		broker1.SetCacheInvalidators(cr.InvalidateNode, cr.InvalidateObserver)
		broker2.SetCacheInvalidators(cr.InvalidateNode, cr.InvalidateObserver)
	}

	var ingestWorkers sync.WaitGroup
	if mode.Ingest {
		ingestWorkers.Go(func() { broker1.Start(ctx) })
		ingestWorkers.Go(func() { broker2.Start(ctx) })
	}

	rollup := background.NewRollup(maintStore, resolved.PacketRetention, resolved.RollupRetention)
	if mode.Jobs {
		go func() {
			if err := rollup.CatchUp(ctx); err != nil {
				slog.Error("analytics rollup catch-up failed", "component", "background", "error", err)
			}
		}()
	}
	tasks := []background.Task{
		rollup.Task(),
		background.ViewRefreshTask(maintStore, resolved.ViewRefreshInterval),
		background.CleanupTask(maintStore, background.CleanupConfig{
			TelemetryRetention: resolved.TelemetryRetention,
			PacketRetention:    resolved.PacketRetention,
			RollupRetention:    resolved.RollupRetention,
			NodeDeleteAfter:    resolved.NodeDeleteAfter,
			Interval:           resolved.CleanupInterval,
		}),
		background.RouteRetentionTask(maintStore, resolved.RouteRetention, resolved.RouteGrace, int64(resolved.RouteMinObservations), resolved.ReconfirmInterval),
		background.ReconfirmTask(maintStore, resolved.RouteReconfirmRowsPerHour, background.ReconfirmTick),
		background.IndexHealthTask(maintStore, time.Hour),
		background.PayloadTypeBackfillTask(maintStore, time.Minute),
		background.PgStatsTask(maintStore, time.Minute),
	}
	if resolved.ObserverDeleteAfter > 0 {
		var onDelete func(context.Context, uuid.UUID)
		if cr, ok := reader.(*cache.CachedReader); ok {
			onDelete = cr.InvalidateObserver
		}
		tasks = append(tasks, background.ObserverCleanupTask(coalescer, resolved.ObserverDeleteAfter, resolved.CleanupInterval, onDelete))
	}
	if mode.MeshMapper && scopeImporter != nil {
		tasks = append(tasks, background.Task{Name: "meshmapper.scopes", Interval: meshmapper.PollInterval, Run: scopeImporter.Refresh})
	}
	if mode.MeshMapper && cfg.MeshMapper.Zones.Enabled {
		tasks = append(tasks, background.Task{Name: "meshmapper.zones", Interval: meshmapper.PollInterval, Run: zones.Refresh})
	}
	if channelImporter != nil && cfg.MeshMapper.Channels.Enabled {
		tasks = append(tasks, background.Task{Name: "meshmapper.channels", Interval: meshmapper.PollInterval, Run: channelImporter.Refresh})
	}
	if repeaterImporter != nil && cfg.MeshMapper.Repeaters.Enabled {
		tasks = append(tasks, background.Task{Name: "meshmapper.repeaters", Interval: meshmapper.PollInterval, Run: repeaterImporter.Refresh})
	}
	cmiJobs := &background.CmiJobs{
		Begin: func(ctx context.Context) (background.CmiSession, bool, error) {
			s, ok, err := maintStore.BeginCmi(ctx)
			if s == nil {
				return nil, ok, err
			}
			return s, ok, err
		},
		Wait: func(ctx context.Context) error {
			return db.WaitForPreBootWriters(ctx, pools.Maint, bootTS, time.Minute)
		},
		Trust:    readModels.Cmi,
		Interval: background.CmiInterval,
	}
	psJobs := &background.PacketSitesJobs{
		Begin: func(ctx context.Context) (background.PacketSitesSession, bool, error) {
			s, ok, err := maintStore.BeginPacketSites(ctx)
			if s == nil {
				return nil, ok, err
			}
			return s, ok, err
		},
		Wait: func(ctx context.Context) error {
			return db.WaitForPreBootWriters(ctx, pools.Maint, bootTS, time.Minute)
		},
		Trust:    readModels.PacketSites,
		Interval: background.PacketSitesInterval,
		Sweep: func(ctx context.Context) (db.SweepResult, error) {
			return maintStore.SweepPacketSitesFronts(ctx, background.PacketSitesSweepLookback)
		},
		SweepPlan: maintStore.PacketSitesSweepPlanOK,
		Bytes:     maintStore.PacketSitesTotalBytes,
	}
	if mode.Jobs {
		tasks = append(tasks, cmiJobs.Task(), psJobs.Task(), psJobs.AuditTask(), psJobs.SweepTask())
	} else {
		go background.CmiReadOnlyWatch(ctx, maintStore.CheckCmiReadOnly, readModels.Cmi, background.CmiInterval)
		go background.PacketSitesReadOnlyWatch(ctx, maintStore.CheckPacketSitesReadOnly, readModels.PacketSites, background.PacketSitesInterval)
	}
	pgSource, pgTask := planguardWiring(ctx, resolved.PlanguardDailyCheck, mode.Jobs, pools.Maint)
	if pgTask != nil {
		tasks = append(tasks, *pgTask)
	}
	profiles := configureProfiling(ctx, pools.Maint)
	defer profiles.Stop()
	for i := range tasks {
		tasks[i].Run = profiles.WrapTask(tasks[i].Name, tasks[i].Run)
	}
	if mode.Jobs {
		scheduler := background.New(tasks)
		go scheduler.Start(ctx)
		go cmiJobs.Boot(ctx)
		go psJobs.Boot(ctx)
	}

	// ── HTTP server ──────────────────────────────────────────────────────────
	// Wrap after wiring cache invalidators and cleanup callbacks to the actual
	// CachedReader. Only response projections receive the geographic annotation.
	reader = api.WithLocalBorders(reader, localBorders)
	r := router.New(h, reader, []*ingest.Worker{broker1, broker2}, router.Options{
		MaxConnsPerIP:        resolved.MaxConnsPerIP,
		MaxConnectsPerMinute: resolved.MaxConnectsPerMinute,
		WSAllowedOrigins:     cfg.WebSocket.AllowedOrigins,
		CORS:                 cfg.CORS, Server: cfg.Server, Auth: cfg.Auth, RateLimit: resolved.RateLimit,
		Scopes: scopes, RollupRetention: resolved.RollupRetention,
		MinAppVersion: api.HigherVersion(api.MinAppVersion, cfg.Mobile.MinAppVersion),
		MinWebVersion: api.HigherVersion(api.MinWebVersion, cfg.Web.MinWebVersion),
		ServerVersion: docs.SwaggerInfo.Version,
		AdminRoutes: map[string]http.Handler{
			"/accounts": handlers.AccountsRouter(maintStore),
			"/backup":   handlers.BackupRouter(backupOpts, ctx),
			"/db":       handlers.DBAdminRouter(maintStore, pgSource),
		},
		Guards:        guard,
		APIPlanLayout: string(layout),
	})

	srv := newHTTPServer(addr, r)
	go func() {
		slog.Info(fmt.Sprintf("Beacon listening on %s", addr), "component", "startup")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "component", "startup", "error", err)
			os.Exit(1)
		}
	}()

	var backgroundJobs sync.WaitGroup
	if decryptBackfill != nil {
		decryptBackfill.RequestAll()
		backgroundJobs.Go(func() { decryptBackfill.Run(ctx) })
	}

	// The admin listener serves only /metrics; keep it off the public network.
	var adminSrv *http.Server
	if resolved.AdminListen != "" {
		adminSrv = newHTTPServer(resolved.AdminListen, metricsMux())
		go func() {
			slog.Info(fmt.Sprintf("metrics listening on %s", resolved.AdminListen), "component", "startup")
			if err := adminSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("metrics server error", "component", "startup", "error", err)
				os.Exit(1)
			}
		}()
	}

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
	if adminSrv != nil {
		if err := adminSrv.Shutdown(shutdownCtx); err != nil {
			slog.Error("metrics server shutdown error", "component", "startup", "error", err)
		}
	}
	ingestWorkers.Wait()
	backgroundJobs.Wait()
	// Run's final flush, then one for the bumps written after it, both before the deferred pools.Close.
	finishPresence(shutdownCtx)
}

// proxyLimitWarning explains why per-IP limits go site-wide behind an unlisted reverse proxy.
func proxyLimitWarning(r config.ResolvedConfig, trustedProxies int) string {
	if trustedProxies > 0 {
		return ""
	}
	limits := "the WebSocket connect limit"
	if r.RateLimit.Enabled {
		limits = "the REST rate limit and the WebSocket connect limit"
	}
	return fmt.Sprintf("warning: server.trusted_proxies is empty, so %s key on the direct peer; behind a reverse proxy every visitor shares one budget. List the proxy in server.trusted_proxies and have it set X-Real-IP", limits)
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

// planguardWiring builds the daily read-only plan check on the maintenance pool, behind planguard.daily_check.
// ctx is the server's root context: its end cancels any run in flight.
// Off, /planguard answers that the check is disabled. On, the check runs with the other jobs (config validation
// refuses it on a read-only server); a corpus that does not load disables it but never stops the server, and the
// source stays a nil interface, so /planguard is 404.
func planguardWiring(ctx context.Context, enabled, jobs bool, maint *pgxpool.Pool) (handlers.PlanguardSource, *background.Task) {
	if !enabled {
		return disabledPlanguard{}, nil
	}
	// On, the last-run timestamp is exported from boot (0 until a run succeeds), so a check that never succeeds,
	// even one whose corpus does not load, still trips the staleness alert.
	metrics.EnablePlanguard()
	// Defensive: configuration refuses planguard.daily_check together with server.read_only, the only mode
	// without jobs, so this branch is not reached today. It keeps a jobless server from ever running the check.
	if !jobs {
		return nil, nil
	}
	checker, err := planguard.NewProdChecker(maint, db.SQLCSources, pgdata.Files)
	if err != nil {
		slog.Error("plan check disabled", "component", "startup", "error", err)
		return nil, nil
	}
	pg := background.NewPlanguard(ctx, checker.Run)
	task := pg.Task(24 * time.Hour)
	return planguardSource{pg}, &task
}

// planguardSource serves the plan check's reports in their API shape.
type planguardSource struct{ pg *background.Planguard }

func (s planguardSource) Last() (api.PlanguardReport, error) {
	rep, ok := s.pg.Last()
	if !ok {
		return api.PlanguardReport{}, api.ErrPlanguardNotRun
	}
	return rep.API(), nil
}

func (s planguardSource) RunNow(ctx context.Context) (api.PlanguardReport, error) {
	rep, err := s.pg.RunNow(ctx)
	if err != nil {
		return api.PlanguardReport{}, err
	}
	return rep.API(), nil
}

// disabledPlanguard answers /planguard when planguard.daily_check is off.
type disabledPlanguard struct{}

func (disabledPlanguard) Last() (api.PlanguardReport, error) {
	return api.PlanguardReport{}, api.ErrPlanguardDisabled
}

func (disabledPlanguard) RunNow(context.Context) (api.PlanguardReport, error) {
	return api.PlanguardReport{}, api.ErrPlanguardDisabled
}
