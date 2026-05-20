package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"tower/internal/api/router"
	"tower/internal/hub"
	"tower/internal/ingest"

	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	// ── Hub ──────────────────────────────────────────────────────────────────
	h := hub.New()
	go h.Run()

	// ── MQTT ingest workers ──────────────────────────────────────────────────
	// TODO: wire in a real DB handle and ChannelKeyStore once those are built.
	// For now the workers start but immediately hit the decode TODO stub.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker1 := ingest.New(
		ingest.Config{
			BrokerName: "mqtt1",
			URL:        mustEnv("MQTT_BROKER_1_URL"),
			Username:   mustEnv("MQTT_BROKER_1_USERNAME"),
			Password:   mustEnv("MQTT_BROKER_1_PASSWORD"),
		},
		nil, // TODO: replace with *db.Queries or pgxpool handle
		h,
		nil, // TODO: replace with channel key store
	)

	broker2 := ingest.New(
		ingest.Config{
			BrokerName: "mqtt2",
			URL:        mustEnv("MQTT_BROKER_2_URL"),
			Username:   mustEnv("MQTT_BROKER_2_USERNAME"),
			Password:   mustEnv("MQTT_BROKER_2_PASSWORD"),
		},
		nil, // TODO: replace with *db.Queries or pgxpool handle
		h,
		nil, // TODO: replace with channel key store
	)

	go broker1.Start(ctx)
	go broker2.Start(ctx)

	// ── HTTP server ──────────────────────────────────────────────────────────
	r := router.New(h)

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

// mustEnv returns the value of an env var, or empty string if unset.
// Workers handle missing config gracefully (log + retry), so we don't fatal here.
func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Printf("warning: %s is not set", key)
	}
	return v
}
