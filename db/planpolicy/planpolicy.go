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
)

// VerificationOnly lists statements no page runs; the API router refuses them. ListChannelMessages and
// ListAllChannelMessages are the original message statements, kept as the channel read model's first-time
// verification oracle (run on its maintenance connection); ListPacketsByIATAs is the original region statement,
// kept so tests can compare region pages with it.
var VerificationOnly = map[string]bool{
	"ListChannelMessages":    true,
	"ListAllChannelMessages": true,
	"ListPacketsByIATAs":     true,
}

// ClassOf returns the plan class for a sqlc statement name.
func ClassOf(name string) Class {
	if Cached[name] {
		return ClassG
	}
	return ClassC
}
