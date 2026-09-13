// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestAccountsPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN for account regression")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	setup, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close(context.Background())
	schema := "account_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := setup.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := setup.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE"); err != nil {
			t.Error(err)
		}
	}()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	sql, err := migrationFiles.ReadFile("migrations/034_accounts.sql")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := applyMigration(ctx, pool, string(sql)); err != nil {
			t.Fatal(err)
		}
	}
	store := New(pool, 0, 0)
	items, err := store.ListAccounts(ctx)
	if err != nil || items == nil || len(items) != 0 {
		t.Fatal("empty list", err)
	}
	if _, err := store.CreateAccount(ctx, "\t"); !errors.Is(err, api.ErrAccountNameInvalid) {
		t.Fatal("invalid name accepted", err)
	}
	first, err := store.CreateAccount(ctx, "  Montréal 🦀 ")
	if err != nil || first.Name != "Montréal 🦀" || !first.Active || first.ID == uuid.Nil || first.CreatedAt.IsZero() {
		t.Fatal("create", err)
	}
	if _, err := store.CreateAccount(ctx, first.Name); !errors.Is(err, api.ErrAccountNameConflict) {
		t.Fatal("duplicate", err)
	}
	if err := store.DeactivateAccount(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	inactive, err := store.GetAccount(ctx, first.ID)
	if err != nil || inactive.Active || inactive.DeactivatedAt == nil {
		t.Fatal("deactivate", err)
	}
	if err := store.DeactivateAccount(ctx, first.ID); !errors.Is(err, api.ErrAccountInactive) {
		t.Fatal("repeated deactivate", err)
	}
	again, _ := store.GetAccount(ctx, first.ID)
	if !again.DeactivatedAt.Equal(*inactive.DeactivatedAt) {
		t.Fatal("deactivation timestamp changed")
	}
	reused, err := store.CreateAccount(ctx, first.Name)
	if err != nil || reused.ID == first.ID {
		t.Fatal("name reuse", err)
	}
	if _, err := store.CreateAccount(ctx, "montréal 🦀"); err != nil {
		t.Fatal("names must be case-sensitive", err)
	}
	missing := uuid.New()
	if _, err := store.GetAccount(ctx, missing); !errors.Is(err, api.ErrAccountNotFound) {
		t.Fatal("missing get", err)
	}
	if err := store.DeactivateAccount(ctx, missing); !errors.Is(err, api.ErrAccountNotFound) {
		t.Fatal("missing deactivate", err)
	}
	items, err = store.ListAccounts(ctx)
	if err != nil || len(items) != 3 {
		t.Fatal("list lifecycle", err)
	}
	for i := 1; i < len(items); i++ {
		if items[i-1].CreatedAt.Before(items[i].CreatedAt) || (items[i-1].CreatedAt.Equal(items[i].CreatedAt) && items[i-1].ID.String() < items[i].ID.String()) {
			t.Fatal("unstable list order")
		}
	}
	start := make(chan struct{})
	results := make(chan error, 8)
	for i := 0; i < 8; i++ {
		go func() { <-start; _, err := store.CreateAccount(ctx, "concurrent"); results <- err }()
	}
	close(start)
	created := 0
	for i := 0; i < 8; i++ {
		err := <-results
		if err == nil {
			created++
		} else if !errors.Is(err, api.ErrAccountNameConflict) {
			t.Fatal("concurrent create", err)
		}
	}
	if created != 1 {
		t.Fatalf("created %d accounts with the same name", created)
	}
	for round := 0; round < 8; round++ {
		account, err := store.CreateAccount(ctx, "race-deactivate")
		if err != nil {
			t.Fatal(err)
		}
		gate := make(chan struct{})
		for i := 0; i < 2; i++ {
			go func() { <-gate; results <- store.DeactivateAccount(ctx, account.ID) }()
		}
		close(gate)
		changed := 0
		for i := 0; i < 2; i++ {
			err := <-results
			if err == nil {
				changed++
			} else if !errors.Is(err, api.ErrAccountInactive) {
				t.Fatal("concurrent deactivate", err)
			}
		}
		if changed != 1 {
			t.Fatalf("deactivated %d times", changed)
		}
	}
}
