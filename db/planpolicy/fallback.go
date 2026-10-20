// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planpolicy

// RestoredFallbacks are the two statements that served region and scope message pages and observer advert pages
// while their read model or backfill was not ready. Today one is verification-only (the router refuses it first)
// and the other no longer exists. They are class Slow so that, if the change that removed the fallbacks is
// reverted, they come back as on the old legacy pool: ungated, on their own connections (api_slow, sized by
// database.slow_pool_size), never holding api_custom's.
var RestoredFallbacks = map[string]bool{
	"ListChannelMessages":           true,
	"ListObserverAdvertsJoinFilter": true,
}
