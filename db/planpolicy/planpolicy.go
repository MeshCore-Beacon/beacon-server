// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package planpolicy decides which plan class each sqlc statement runs under and
// which API pool layout the server opens.
package planpolicy

import "errors"

// Layout selects how API statements are spread across connection pools.
type Layout string

const (
	// CustomAll is the default: every API statement gets a custom plan.
	CustomAll Layout = "custom_all"
	// Split runs statements proven generic-safe on cached generic plans.
	Split Layout = "split"
	// Single is the rollback layout: one API pool, and every guard family observes.
	Single Layout = "single"
)

var errLayout = errors.New("database.api_plan_layout must be custom_all, split or single")

// ParseLayout reads database.api_plan_layout; empty means CustomAll.
func ParseLayout(s string) (Layout, error) {
	switch l := Layout(s); l {
	case "":
		return CustomAll, nil
	case CustomAll, Split, Single:
		return l, nil
	}
	return "", errLayout
}

// Class is the plan class of one sqlc statement.
type Class int

const (
	// ClassC gets a custom plan on every execution: the default for every name.
	ClassC Class = iota
	// ClassG is admitted as generic-safe and runs on a cached generic plan under Split.
	ClassG
	// ClassLegacy is a statement not yet rewritten: it runs on api_legacy, unguarded.
	ClassLegacy
)

// Legacy lists the statements not yet rewritten. Each is removed in the change that replaces it.
var Legacy = map[string]bool{
	"ListPacketsByIATAs":     true,
	"ListChannelMessages":    true,
	"ListAllChannelMessages": true,
	"GetStatsSeries":         true,
	"GetPathStats":           true,
	"GetTopNodes":            true,
	"GetStatsTopAdvertisers": true,
	"ListObserverAdverts":    true,
	"ListTraceTags":          true,
}

// Undecided, needs the maintainer's approval before the legacy pool is removed: a Fallback map for the
// untrusted read-model fallbacks (ListPacketsByIATAs, ListChannelMessages, ListAllChannelMessages) would
// go here, as a fourth class that the router sends to api_custom like C but never gates, so no deadline
// ever cuts it.

// ClassOf returns the plan class for a sqlc statement name.
func ClassOf(name string) Class {
	switch {
	case Legacy[name]:
		return ClassLegacy
	case Cached[name]:
		return ClassG
	}
	return ClassC
}
