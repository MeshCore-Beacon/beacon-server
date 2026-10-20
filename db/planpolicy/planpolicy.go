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
	// ClassSlow is a statement that may read whole site histories in one call: it runs on api_slow, ungated, under
	// that pool's own statement limit.
	ClassSlow
)

// Slow lists the statements that may walk a large part of the observations in one call, so they run on their own
// small pool and cannot hold the API pool's connections: the region page statement that answers when the site read
// model's bounded reads run out of budget (both cursor forms), and the all-channel message statement for sites with a
// since or a scope, which no read-model statement serves.
var Slow = map[string]bool{
	"ListPacketsByIATAs":      true,
	"ListPacketsByIATAsAfter": true,
	"ListAllChannelMessages":  true,
}

// ClassOf returns the plan class for a sqlc statement name.
func ClassOf(name string) Class {
	switch {
	case Slow[name], RestoredFallbacks[name]:
		return ClassSlow
	case Cached[name]:
		return ClassG
	}
	return ClassC
}
