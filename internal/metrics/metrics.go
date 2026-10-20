// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package metrics holds the server's Prometheus collectors and the handler
// that serves them.
package metrics

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
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

	// PlanguardLastRun is when the daily plan check last finished. A server with the check off carries no
	// series; with it on (EnablePlanguard) the series is 0 until the first run.
	PlanguardLastRun = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "beacon_planguard_last_run_timestamp_seconds",
		Help: "Unix time the daily plan check last finished.",
	})

	// ReadModelRepaired counts the rows each read model's reconcile wrote: anything above zero was missed by a writer
	// that bypassed the model.
	ReadModelRepaired = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_readmodel_repaired_rows_total",
		Help: "Rows the read-model reconcile repaired, by model.",
	}, []string{"model"})

	// ReadModelTrusted is 1 while this process serves pages from a read model, by model.
	ReadModelTrusted = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "beacon_readmodel_trusted",
		Help: "1 while this process serves pages from the read model, 0 while it serves the original statements.",
	}, []string{"model"})
)

var planguardEnabled atomic.Bool

// EnablePlanguard exports PlanguardLastRun from now on, 0 until the first run, so a staleness alert on it fires
// for a check that is on but never succeeds. Without it the gauge is exported only once set.
func EnablePlanguard() { planguardEnabled.Store(true) }

// setGauge exports its gauge once it holds a non-zero value, or always once enabled reports true.
type setGauge struct {
	prometheus.Gauge
	enabled *atomic.Bool
}

func (g setGauge) Collect(ch chan<- prometheus.Metric) {
	if !g.enabled.Load() {
		var m dto.Metric
		if err := g.Write(&m); err != nil || m.GetGauge().GetValue() == 0 {
			return
		}
	}
	ch <- g.Gauge
}

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

var watermarkAgeDesc = prometheus.NewDesc("beacon_readmodel_watermark_age_seconds",
	"Seconds since this process last finished a reconcile of the read model, by model.", []string{"model"}, nil)

// watermarkCollector emits the age of each model's last finished reconcile at scrape time.
type watermarkCollector struct {
	mu sync.Mutex
	at map[string]time.Time
}

func (c *watermarkCollector) Describe(ch chan<- *prometheus.Desc) { ch <- watermarkAgeDesc }

func (c *watermarkCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for model, at := range c.at {
		ch <- prometheus.MustNewConstMetric(watermarkAgeDesc, prometheus.GaugeValue, time.Since(at).Seconds(), model)
	}
}

var watermarks = &watermarkCollector{at: map[string]time.Time{}}

// SetReadModelWatermark records when model's reconcile last finished.
func SetReadModelWatermark(model string, at time.Time) {
	watermarks.mu.Lock()
	watermarks.at[model] = at
	watermarks.mu.Unlock()
}

func init() {
	Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		GuardWouldRefuse, GuardRefused, GuardDB, DBTime, HTTPRequest, PoolAcquire,
		IngestQueueDepth, IngestDropped, JobDuration, JobRows, IndexBytes, IndexBloat,
		PlanguardFailures, setGauge{PlanguardLastRun, &planguardEnabled}, pgActivity, ReadModelRepaired, ReadModelTrusted, watermarks,
	)
}

// Handler serves the Registry in the Prometheus exposition format.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{})
}
