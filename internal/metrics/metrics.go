// Package metrics holds the Prometheus metrics of the service. Label values are bounded:
// sources come from config, codes and reasons from fixed sets; per-entry series are capped.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const ns = "datasource_cache"

// Registry is separate from the default one to enable extended Go runtime metrics.
var Registry = prometheus.NewRegistry()

var f = promauto.With(Registry)

func init() {
	Registry.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		// GC pauses, scheduler latency and memory classes: enough to tell GC pressure from CPU throttling.
		collectors.NewGoCollector(collectors.WithGoCollectorRuntimeMetrics(
			collectors.MetricsGC, collectors.MetricsMemory, collectors.MetricsScheduler,
		)),
	)
}

func Handler() http.Handler {
	return promhttp.InstrumentMetricHandler(Registry, promhttp.HandlerFor(Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
		Registry:          Registry,
		// A failing collector must not turn the whole scrape into a 500.
		ErrorHandling: promhttp.ContinueOnError,
	}))
}

var (
	latencyBuckets  = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 180}
	upstreamBuckets = []float64{.25, .5, 1, 2.5, 5, 10, 30, 60, 120, 180, 300}
	ageBuckets      = []float64{60, 300, 900, 1800, 3600, 7200, 21600, 43200, 86400, 259200, 604800}
)

// Client side.
var (
	Requests = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "requests_total",
		Help: "Client requests by cache result (hit, stale, miss, bypass, error, canceled) and response code; canceled is 499.",
	}, []string{"source", "result", "code"})

	RequestDuration = f.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "request_duration_seconds",
		Help:    "Client request latency including the response write; miss and error include the upstream wait.",
		Buckets: latencyBuckets,
	}, []string{"source", "result"})

	RequestsInFlight = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "requests_in_flight",
		Help: "Client requests being served.",
	}, []string{"source"})

	ResponseBytes = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "response_bytes_total",
		Help: "Body bytes sent to clients; identity means decompressed on the fly.",
	}, []string{"source", "encoding"})

	ServedAge = f.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "served_age_seconds",
		Help:    "Age of the data at the moment it was served.",
		Buckets: ageBuckets,
	}, []string{"source", "result"})

	Rejected = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "rejected_requests_total",
		Help: "Requests refused before the cache: unknown_source, unsafe_path, not_allowed, body_too_large.",
	}, []string{"reason"})

	Coalesced = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "coalesced_requests_total",
		Help: "Cold requests that joined a fetch already running in this replica.",
	}, []string{"source"})
)

// Cache internals.
var (
	Refreshes = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "refreshes_total",
		Help: "Refresh attempts by trigger (miss, stale, scheduled) and result: success, error, " +
			"backoff (skipped after a recent failure), locked (another replica is fetching), " +
			"current (another replica had just refreshed it, no upstream call).",
	}, []string{"source", "trigger", "result"})

	LockLost = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "lock_lost_total",
		Help: "Fetch lock leases that expired before the fetch ended; another replica may fetch the same key.",
	}, []string{"source"})

	RefreshDuration = f.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "refresh_duration_seconds",
		Help:    "Fetch under the lock including retries, rate limiter waits and the store write.",
		Buckets: upstreamBuckets,
	}, []string{"source", "trigger"})

	LockWaits = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "lock_waits_total",
		Help: "Cold requests that waited for another replica, by outcome: entry, failure, released, timeout.",
	}, []string{"source", "outcome"})

	LockWaitDuration = f.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "lock_wait_duration_seconds",
		Help:    "Time spent waiting for another replica's fetch.",
		Buckets: upstreamBuckets,
	}, []string{"source"})

	Lookups = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "lookups_total",
		Help: "Client lookups: local (replica memory, no store call), loaded (no local copy, read from store), " +
			"absent (nowhere), error (no local copy and store unavailable).",
	}, []string{"result"})

	Revalidations = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "revalidations_total",
		Help: "Background checks of a local copy against the store: current, updated (new meta, same body), " +
			"loaded (newer body), kept (store copy missing or older), error.",
	}, []string{"result"})

	Entries = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "entries",
		Help: "Tracked keys by state: fresh (younger than TTL), stale (within max stale), missing (no usable data).",
	}, []string{"source", "state"})

	EntriesFailing = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "entries_failing",
		Help: "Tracked keys whose last refresh failed.",
	}, []string{"source"})

	EntryMaxAge = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "entry_max_age_seconds",
		Help: "Age of the oldest tracked entry; a key without data counts as max stale.",
	}, []string{"source"})

	StoredBytes = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "stored_bytes",
		Help: "Body bytes of tracked entries: gzip is what the store holds, identity is the uncompressed size.",
	}, []string{"source", "encoding"})

	RefreshCycleLast = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "refresh_cycle_last_timestamp_seconds",
		Help: "Last scheduling pass of a source. A pass waits only for a free concurrency slot, so a stalled " +
			"value means every slot is held by a hung fetch or the loop is stuck.",
	}, []string{"source"})

	LocalEvictions = f.NewCounter(prometheus.CounterOpts{
		Namespace: ns, Name: "local_evictions_total",
		Help: "Replica copies dropped to stay within CACHE_LOCAL_MAX_BYTES; the next request rereads them from the store.",
	})

	RefreshesInFlight = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "refreshes_in_flight",
		Help: "Background key refreshes running in this replica; stuck at <NAME>_CONCURRENCY means new keys wait.",
	}, []string{"source"})

	// Per-entry series, emitted by the cache collector up to METRICS_ENTRY_LIMIT.
	EntryAgeDesc = prometheus.NewDesc(ns+"_entry_age_seconds",
		"Age of one cached response.", []string{"source", "request"}, nil)
	EntrySizeDesc = prometheus.NewDesc(ns+"_entry_size_bytes",
		"Uncompressed body size of one cached response.", []string{"source", "request"}, nil)
	EntryAccessDesc = prometheus.NewDesc(ns+"_entry_last_access_timestamp_seconds",
		"Last client request for one key (tracked once a minute).", []string{"source", "request"}, nil)
	EntryFailingDesc = prometheus.NewDesc(ns+"_entry_failing",
		"1 if the last refresh of this key failed.", []string{"source", "request"}, nil)
	LocalEntriesDesc = prometheus.NewDesc(ns+"_local_entries",
		"Entries held in this replica's memory.", nil, nil)
	LocalBytesDesc = prometheus.NewDesc(ns+"_local_bytes",
		"Gzipped body bytes held in this replica's memory.", nil, nil)
)

// Upstream.
var (
	UpstreamRequests = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "upstream_requests_total",
		Help: "Upstream attempts by HTTP code; code=\"error\" means no response.",
	}, []string{"source", "code"})

	UpstreamDuration = f.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "upstream_request_duration_seconds",
		Help:    "One upstream attempt, from sending the request to reading the whole body.",
		Buckets: upstreamBuckets,
	}, []string{"source"})

	UpstreamErrors = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "upstream_errors_total",
		Help: "Failed upstream attempts by reason: timeout, eof, reset, refused, dns, tls, canceled, other, " +
			"http_4xx, http_429, http_5xx, invalid_json, too_large, rate_limiter, auth_secret.",
	}, []string{"source", "reason"})

	UpstreamRetries = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "upstream_retries_total",
		Help: "Upstream attempts after the first one.",
	}, []string{"source"})

	UpstreamInFlight = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "upstream_requests_in_flight",
		Help: "Upstream attempts in progress in this replica.",
	}, []string{"source"})

	UpstreamRateLimitWait = f.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "upstream_rate_limit_wait_seconds",
		Help:    "Time an attempt waited for the source rate limiter.",
		Buckets: []float64{.001, .01, .1, .5, 1, 5, 10, 30, 60},
	}, []string{"source"})

	UpstreamSlotWait = f.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "upstream_slot_wait_seconds",
		Help:    "Time an attempt waited for a free slot of the source's max_in_flight.",
		Buckets: []float64{.001, .01, .1, .5, 1, 5, 10, 30, 60},
	}, []string{"source"})

	UpstreamResponseSize = f.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "upstream_response_size_bytes",
		Help:    "Uncompressed upstream body size; _sum is the traffic downloaded.",
		Buckets: prometheus.ExponentialBuckets(1024, 4, 9),
	}, []string{"source"})

	UpstreamLastSuccess = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "upstream_last_success_timestamp_seconds",
		Help: "Last successful fetch made by this replica; take max over replicas.",
	}, []string{"source"})
)

// Store.
var (
	StoreDuration = f.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: ns, Name: "store_operation_duration_seconds",
		Help:    "Store (Redis) operation latency.",
		Buckets: []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 3},
	}, []string{"op"})

	StoreErrors = f.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns, Name: "store_errors_total",
		Help: "Failed store operations; the replica keeps serving from memory.",
	}, []string{"op"})

	StoreUp = f.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "store_up",
		Help: "1 if the store answered the last ping of the refresh pass.",
	})

	RedisPoolHitsDesc = prometheus.NewDesc(ns+"_redis_pool_hits_total",
		"Times a free connection was found in the pool.", nil, nil)
	RedisPoolMissesDesc = prometheus.NewDesc(ns+"_redis_pool_misses_total",
		"Times a new connection had to be dialed.", nil, nil)
	RedisPoolTimeoutsDesc = prometheus.NewDesc(ns+"_redis_pool_timeouts_total",
		"Times waiting for a pool connection timed out.", nil, nil)
	RedisPoolWaitsDesc = prometheus.NewDesc(ns+"_redis_pool_waits_total",
		"Times a request waited for a pool connection.", nil, nil)
	RedisPoolWaitSecondsDesc = prometheus.NewDesc(ns+"_redis_pool_wait_seconds_total",
		"Total time spent waiting for pool connections.", nil, nil)
	RedisPoolConnsDesc = prometheus.NewDesc(ns+"_redis_pool_connections",
		"Pool connections by state: total, idle.", []string{"state"}, nil)
	RedisPoolStaleDesc = prometheus.NewDesc(ns+"_redis_pool_stale_connections_total",
		"Stale connections removed from the pool.", nil, nil)
)

// Configuration as metrics, so alerts compare against the deployed values instead of hardcoding them.
var (
	SourceTTL = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "source_ttl_seconds", Help: "Freshness TTL of the source.",
	}, []string{"source"})

	SourceTimeout = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "source_timeout_seconds", Help: "Per-attempt upstream timeout of the source.",
	}, []string{"source"})

	SourceConcurrency = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "source_concurrency", Help: "Keys of the source refreshed in parallel by one replica.",
	}, []string{"source"})

	SourceMaxKeys = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "source_max_keys", Help: "Tracked keys of the source besides warmup; new keys above it bypass the cache.",
	}, []string{"source"})

	SourceMissWait = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "source_miss_wait_seconds", Help: "How long a cold request to the source waits before 504.",
	}, []string{"source"})

	SourceMaxInFlight = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "source_max_in_flight", Help: "Upstream requests of the source at once, per replica.",
	}, []string{"source"})

	SourceRateLimit = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "source_rate_limit_rps", Help: "Per-replica request rate limit of the source, 0 = unlimited.",
	}, []string{"source"})

	ConfigMaxStale = f.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "config_max_stale_seconds", Help: "CACHE_MAX_STALE.",
	})
	ConfigIdleTTL = f.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "config_idle_ttl_seconds", Help: "CACHE_IDLE_TTL.",
	})
	ConfigErrorBackoff = f.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "config_error_backoff_seconds", Help: "CACHE_ERROR_BACKOFF.",
	})
	ConfigRefreshInterval = f.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "config_refresh_interval_seconds", Help: "CACHE_REFRESH_INTERVAL.",
	})
	ConfigMissWait = f.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "config_miss_wait_seconds", Help: "CACHE_MISS_WAIT.",
	})
	ConfigLocalMaxBytes = f.NewGauge(prometheus.GaugeOpts{
		Namespace: ns, Name: "config_local_max_bytes", Help: "CACHE_LOCAL_MAX_BYTES.",
	})

	BuildInfo = f.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "build_info", Help: "Always 1.",
	}, []string{"version", "revision", "goversion"})
)
