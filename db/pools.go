// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"fmt"

	"github.com/MeshCore-Beacon/beacon-server/db/planpolicy"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool names. Each is also the pool's metric label and, prefixed with "beacon-", its application_name.
const (
	PoolAPILegacy = "api_legacy"
	PoolAPICustom = "api_custom"
	PoolAPICached = "api_cached"
	PoolIngest    = "ingest"
	PoolMaint     = "maint"
)

// Pools holds one pgx pool per workload. A pool is nil when the layout does not open it.
type Pools struct {
	APILegacy, APICustom, APICached, Ingest, Maint *pgxpool.Pool
}

type poolSpec struct {
	name   string
	size   map[planpolicy.Layout]int32 // a layout absent here does not open the pool
	params map[string]string
}

// poolSpecs is the fixed size and session settings of each pool. api_legacy keeps today's
// session settings: only its application_name differs.
var poolSpecs = []poolSpec{
	{
		name: PoolAPILegacy,
		size: map[planpolicy.Layout]int32{planpolicy.CustomAll: 8, planpolicy.Split: 8, planpolicy.Single: 8},
		params: map[string]string{
			"application_name": "beacon-api_legacy",
		},
	},
	{
		name: PoolAPICustom,
		size: map[planpolicy.Layout]int32{planpolicy.CustomAll: 8, planpolicy.Split: 7},
		params: map[string]string{
			"application_name":                    "beacon-api_custom",
			"plan_cache_mode":                     "force_custom_plan",
			"jit":                                 "off",
			"max_parallel_workers_per_gather":     "0",
			"statement_timeout":                   "35s",
			"idle_in_transaction_session_timeout": "30s",
		},
	},
	{
		name: PoolAPICached,
		size: map[planpolicy.Layout]int32{planpolicy.Split: 5},
		params: map[string]string{
			"application_name":                    "beacon-api_cached",
			"plan_cache_mode":                     "force_generic_plan",
			"jit":                                 "off",
			"max_parallel_workers_per_gather":     "0",
			"statement_timeout":                   "35s",
			"idle_in_transaction_session_timeout": "30s",
		},
	},
	{
		name: PoolIngest,
		size: map[planpolicy.Layout]int32{planpolicy.CustomAll: 6, planpolicy.Split: 6, planpolicy.Single: 6},
		params: map[string]string{
			"application_name":  "beacon-ingest",
			"plan_cache_mode":   "auto",
			"jit":               "off",
			"statement_timeout": "30s",
		},
	},
	{
		name: PoolMaint,
		size: map[planpolicy.Layout]int32{planpolicy.CustomAll: 4, planpolicy.Split: 4, planpolicy.Single: 4},
		params: map[string]string{
			"application_name":                "beacon-maint",
			"plan_cache_mode":                 "force_custom_plan",
			"jit":                             "off",
			"max_parallel_workers_per_gather": "0",
		},
	},
}

func (p *Pools) slot(name string) **pgxpool.Pool {
	switch name {
	case PoolAPILegacy:
		return &p.APILegacy
	case PoolAPICustom:
		return &p.APICustom
	case PoolAPICached:
		return &p.APICached
	case PoolIngest:
		return &p.Ingest
	case PoolMaint:
		return &p.Maint
	}
	panic("db: unknown pool " + name)
}

// OpenPools opens the pools the layout uses, each with its fixed size and session settings
// merged over the DSN's own parameters. Each tune func may adjust a pool's config before it opens.
// On error it closes the pools it already opened. Errors name the pool but never include the DSN,
// since parse errors can embed its password.
func OpenPools(ctx context.Context, dsn string, layout planpolicy.Layout, tune ...func(pool string, cfg *pgxpool.Config)) (*Pools, error) {
	if _, ok := poolSpecs[0].size[layout]; !ok {
		return nil, fmt.Errorf("unknown API plan layout %q", layout)
	}
	p := &Pools{}
	for _, spec := range poolSpecs {
		size, ok := spec.size[layout]
		if !ok {
			continue
		}
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("pool %s: invalid PostgreSQL connection configuration", spec.name)
		}
		cfg.MaxConns = size
		for k, v := range spec.params {
			cfg.ConnConfig.RuntimeParams[k] = v
		}
		cfg.ConnConfig.Tracer = newTracer(spec.name)
		for _, f := range tune {
			f(spec.name, cfg)
		}
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("pool %s: %w", spec.name, err)
		}
		*p.slot(spec.name) = pool
	}
	return p, nil
}

// Close closes every opened pool.
func (p *Pools) Close() {
	for _, spec := range poolSpecs {
		if s := p.slot(spec.name); *s != nil {
			(*s).Close()
			*s = nil
		}
	}
}

// Total is the sum of MaxConns over the opened pools.
func (p *Pools) Total() int32 {
	var n int32
	for _, spec := range poolSpecs {
		if pool := *p.slot(spec.name); pool != nil {
			n += pool.Config().MaxConns
		}
	}
	return n
}
