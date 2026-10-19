// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package metrics holds the server's Prometheus collectors and the handler
// that serves them.
package metrics

import (
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	dbBuckets   = []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}
	httpBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}
)

// Registry holds every Beacon collector plus the Go runtime and process collectors.
var Registry = prometheus.NewRegistry()

var (
	// GuardWouldRefuse counts requests a guard would have refused if it enforced.
	GuardWouldRefuse = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_guard_would_refuse_total",
		Help: "Requests a guard would refuse if it enforced, by family and guard.",
	}, []string{"family", "guard"})

	// GuardRefused counts requests a guard actually refused.
	GuardRefused = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_guard_refused_total",
		Help: "Requests a guard refused, by family and guard.",
	}, []string{"family", "guard"})

	// GuardDB is the database time charged to each guarded request.
	GuardDB = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "beacon_guard_db_seconds",
		Help:    "Database time per guarded request, by family and class.",
		Buckets: dbBuckets,
	}, []string{"family", "class"})

	// DBTime is the duration of each database statement.
	DBTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "beacon_db_time_seconds",
		Help:    "Statement duration, by sqlc statement name and pool.",
		Buckets: dbBuckets,
	}, []string{"statement", "pool"})

	// HTTPRequest is the duration of each HTTP request.
	HTTPRequest = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "beacon_http_request_seconds",
		Help:    "HTTP request duration, by route pattern and guard family.",
		Buckets: httpBuckets,
	}, []string{"route", "family"})

	// PoolAcquire is the time spent waiting for a pooled connection.
	PoolAcquire = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "beacon_pool_acquire_seconds",
		Help:    "Time to acquire a pooled connection, by pool.",
		Buckets: []float64{.0001, .001, .005, .025, .1, .5, 1, 2.5, 5, 10},
	}, []string{"pool"})

	// IngestQueueDepth is the number of messages waiting in the ingest queue.
	IngestQueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "beacon_ingest_queue_depth",
		Help: "Messages waiting in the ingest queue.",
	})

	// IngestDropped counts messages the ingest queue dropped because it was full.
	IngestDropped = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "beacon_ingest_dropped_total",
		Help: "Messages dropped because the ingest queue was full.",
	})

	// JobDuration is the duration of each background job run.
	JobDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "beacon_job_duration_seconds",
		Help:    "Background job run duration, by job.",
		Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1200, 3600},
	}, []string{"job"})

	// JobRows counts the rows each background job wrote or deleted.
	JobRows = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_job_rows_total",
		Help: "Rows written or deleted by background jobs, by job.",
	}, []string{"job"})

	// IndexBytes is the on-disk size of each tracked index.
	IndexBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "beacon_index_bytes",
		Help: "Index size in bytes, by index.",
	}, []string{"index"})

	// IndexBloat is the estimated bloat ratio of each tracked index.
	IndexBloat = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "beacon_index_bloat_ratio",
		Help: "Estimated index bloat ratio, by index.",
	}, []string{"index"})

	// PlanguardFailures marks each corpus entry and plan mode that failed the last daily plan check.
	PlanguardFailures = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "beacon_planguard_failures",
		Help: "1 for each corpus entry and plan mode whose last daily plan check failed.",
	}, []string{"entry", "mode"})

	// PlanguardLastRun is when the daily plan check last finished; 0 until its first run.
	PlanguardLastRun = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "beacon_planguard_last_run_timestamp_seconds",
		Help: "Unix time the daily plan check last finished.",
	})
)

// PgActivity is one snapshot of PostgreSQL-wide activity counters.
type PgActivity struct {
	WALBytes             int64
	CheckpointsTimed     int64
	CheckpointsRequested int64
	XIDAge               int64
}

var (
	walBytesDesc = prometheus.NewDesc("beacon_pg_wal_bytes_total",
		"WAL bytes PostgreSQL has generated.", nil, nil)
	checkpointsDesc = prometheus.NewDesc("beacon_pg_checkpoints_total",
		"Checkpoints PostgreSQL has run, by kind.", []string{"kind"}, nil)
	xidAgeDesc = prometheus.NewDesc("beacon_pg_xid_age",
		"Age of the oldest transaction ID in the database.", nil, nil)
)

// pgActivityCollector emits the last PgActivity snapshot as const metrics. It
// emits nothing until SetPgActivity has been called once.
type pgActivityCollector struct {
	last atomic.Pointer[PgActivity]
}

func (c *pgActivityCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- walBytesDesc
	ch <- checkpointsDesc
	ch <- xidAgeDesc
}

func (c *pgActivityCollector) Collect(ch chan<- prometheus.Metric) {
	a := c.last.Load()
	if a == nil {
		return
	}
	ch <- prometheus.MustNewConstMetric(walBytesDesc, prometheus.CounterValue, float64(a.WALBytes))
	ch <- prometheus.MustNewConstMetric(checkpointsDesc, prometheus.CounterValue, float64(a.CheckpointsTimed), "timed")
	ch <- prometheus.MustNewConstMetric(checkpointsDesc, prometheus.CounterValue, float64(a.CheckpointsRequested), "requested")
	ch <- prometheus.MustNewConstMetric(xidAgeDesc, prometheus.GaugeValue, float64(a.XIDAge))
}

var pgActivity = &pgActivityCollector{}

// SetPgActivity stores the latest PostgreSQL activity snapshot for the next scrape.
func SetPgActivity(a PgActivity) {
	pgActivity.last.Store(&a)
}

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		GuardWouldRefuse, GuardRefused, GuardDB, DBTime, HTTPRequest, PoolAcquire,
		IngestQueueDepth, IngestDropped, JobDuration, JobRows, IndexBytes, IndexBloat,
		PlanguardFailures, PlanguardLastRun, pgActivity,
	)
}

// Handler serves the Registry in the Prometheus exposition format.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{})
}
