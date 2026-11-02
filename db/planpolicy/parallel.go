// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planpolicy

// ParallelWorkers lists the API statements that may use parallel workers, with the max_parallel_workers_per_gather
// each runs with. The API pools set it to 0; the plan router raises it for these statements alone, with SET LOCAL
// inside a transaction of their own (the rollback layout's api_legacy keeps the server's setting).
//
// GetTopNodesAll aggregates every rollup row in the window (about 2M a week); with 2 workers it runs in about half
// the time.
var ParallelWorkers = map[string]int{
	"GetTopNodesAll": 2,
}
