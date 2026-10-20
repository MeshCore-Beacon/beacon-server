// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planpolicy

// ReadModelFallbacks are the two statements that served per-channel region and scope message pages and observer
// advert pages while their read model or backfill was not ready. Today one is verification-only (the router refuses
// it first) and the other no longer exists. They stay class Slow so that, if the change that removed the fallbacks
// is reverted, they come back as on the old legacy pool: ungated, on their own connections (api_slow, whose default
// that revert also restores to 8), never holding api_custom's.
var ReadModelFallbacks = map[string]bool{
	"ListChannelMessages":           true,
	"ListObserverAdvertsJoinFilter": true,
}
