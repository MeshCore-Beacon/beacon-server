// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planpolicy

// ReadModelFallbacks are the statements that serve per-channel region and scope message pages while the channel
// read model is not trusted, and observer advert pages until every observation carries its payload type. They are
// class Slow, so they run as on the old legacy pool: ungated and on their own connections (api_slow, sized by
// database.slow_pool_size), never holding api_custom's. The region and all-channel fallbacks are in Slow itself.
var ReadModelFallbacks = map[string]bool{
	"ListChannelMessages":           true,
	"ListObserverAdvertsJoinFilter": true,
}
