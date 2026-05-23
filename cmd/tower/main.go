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
	"syscall"

	"github.com/MeshCore-Tower/tower-server/db"
	"github.com/MeshCore-Tower/tower-server/internal/api/router"
	"github.com/MeshCore-Tower/tower-server/internal/config"
	"github.com/MeshCore-Tower/tower-server/internal/hub"
	"github.com/MeshCore-Tower/tower-server/internal/ingest"
	"github.com/MeshCore-Tower/tower-server/internal/keystore"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

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

	store := db.New(pool)

	// ── Seed config data ─────────────────────────────────────────────────────
	if err := config.Seed(ctx, cfg, store); err != nil {
		log.Fatalf("failed to seed config: %v", err)
	}

	channelKeys := make(map[string][][]byte)
	for hash, keyHex := range cfg.ChannelKeys {
		key, err := hex.DecodeString(keyHex)
		if err != nil {
			log.Printf("warning: invalid channel key for hash %s, skipping: %v", hash, err)
			continue
		}
		// skip if exact pair already exists
		duplicate := false
		for _, existing := range channelKeys[hash] {
			if bytes.Equal(existing, key) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			channelKeys[hash] = append(channelKeys[hash], key)
		}
	}
	keys := keystore.NewMapKeyStore(channelKeys)

	broker1 := ingest.New(
		ingest.Config{
			BrokerName: "mqtt1",
			URL:        mustEnv("MQTT_BROKER_1_URL"),
			Username:   mustEnv("MQTT_BROKER_1_USERNAME"),
			Password:   mustEnv("MQTT_BROKER_1_PASSWORD"),
		},
		store,
		h,
		keys,
	)

	broker2 := ingest.New(
		ingest.Config{
			BrokerName: "mqtt2",
			URL:        mustEnv("MQTT_BROKER_2_URL"),
			Username:   mustEnv("MQTT_BROKER_2_USERNAME"),
			Password:   mustEnv("MQTT_BROKER_2_PASSWORD"),
		},
		store,
		h,
		keys,
	)

	go broker1.Start(ctx)
	go broker2.Start(ctx)

	// ── HTTP server ──────────────────────────────────────────────────────────
	r := router.New(h, store)

	srv := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	go func() {
		fmt.Printf("Tower listening on %s\n", addr)
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
