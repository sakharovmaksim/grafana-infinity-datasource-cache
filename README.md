# grafana-infinity-datasource-cache

[![CI](https://github.com/sakharovmaksim/grafana-infinity-datasource-cache/actions/workflows/ci.yml/badge.svg)](https://github.com/sakharovmaksim/grafana-infinity-datasource-cache/actions/workflows/ci.yml)
[![Docker Hub](https://img.shields.io/docker/v/sakharovmaksim/grafana-infinity-datasource-cache?sort=semver&label=docker)](https://hub.docker.com/r/sakharovmaksim/grafana-infinity-datasource-cache)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A caching proxy for slow and unstable HTTP APIs queried by the
[Grafana Infinity datasource](https://github.com/grafana/grafana-infinity-datasource). Infinity calls the upstream on
every panel load and keeps no cache, so a rate-limited or flaky API (NVD, OSV, Shodan CVEDB, endoflife.date, raw files
on GitHub…) makes dashboards slow or empty. The proxy mirrors upstream paths, so a datasource is switched by changing
its URL; dashboards stay as they are:

```
/cvedb/cves?product=go&limit=1000      →  https://cvedb.shodan.io/cves?product=go&limit=1000
/nvd/rest/json/cves/2.0?noRejected=&…  →  https://services.nvd.nist.gov/rest/json/cves/2.0?noRejected=&…
```

- Stale-while-revalidate with refresh-ahead: clients get cached data immediately, popular keys never expire.
- During an upstream outage the last good copy is served up to `max_stale`, then the error is explicit.
- Replicas share data and fetch each key once through Redis; without Redis each replica serves from its memory.
- API keys are added by the proxy and stay out of Grafana; paths and methods are allowlisted per source.
- Prometheus metrics, with the configuration exported as metrics for threshold-free alerts.

## Quick start

```
docker run --rm -p 8080:8080 -v "$PWD/config.example.yml:/config.yml:ro" \
  sakharovmaksim/grafana-infinity-datasource-cache -config /config.yml
curl -si 'localhost:8080/cvedb/cves?product=nginx&limit=10' | grep X-Cache   # MISS, then HIT
curl -s localhost:8080/-/entries
```

Redis is optional: if `redis.addr` is omitted or empty and `REDIS_ADDR` is unset, the service starts with an in-memory
store and logs a warning. At least one source must be configured in either mode. With one replica, the in-memory
store supports caching, background refresh and stale responses; cached data is lost on restart. Multiple replicas
can also run without Redis, but each keeps and refreshes its own cache. Configure a shared Redis to share cached
data and coordinate upstream fetches between replicas.

Kubernetes — see the [chart](charts/grafana-infinity-datasource-cache/README.md):

```
helm install infinity-cache oci://ghcr.io/sakharovmaksim/charts/grafana-infinity-datasource-cache \
  --namespace grafana --values my-values.yaml
```

## Grafana setup

- A datasource with relative paths in its queries is switched by its URL alone: set it to
  `http://<proxy>/<source>[/<base path>]` and the allowed hosts to `http://<proxy>`.
- Remove the API key header from the datasource: client headers are not forwarded, the proxy adds the key itself.
- Infinity prepends the datasource URL to any query URL that does not start with it. With absolute URLs in queries,
  switch in this order to keep panels working: queries → absolute proxy URLs (they work with any datasource URL) →
  datasource URL → queries → relative paths; allowed hosts → the proxy only.
- `warmup` entries must match what Infinity sends: it sorts parameters and sends a valueless flag as `flag=`, while
  `flag` and `flag=` are different keys. `/-/entries` shows the request of the first client of each key.
- Keep `miss_wait` below the datasource timeout (60 s by default), otherwise Grafana gives up before the proxy answers.
- Infinity does not show response headers; the state of each key is in `/-/entries` and in the metrics.

## How it works

**Serving.**
- Fresh and stale data are served immediately from replica memory; stale data is refreshed in the background. A client
  waits for the upstream only when there is no data, and not longer than `miss_wait`: then 504, while the fetch goes on.
- Keys requested within `idle_ttl` are refreshed before `ttl` runs out, so clients see HIT. `warmup` keys are loaded at
  start and kept forever.
- While the upstream is down, the last copy not older than `max_stale` is served, then 502 (504 if the upstream did not
  answer in time). All replicas together retry a failed request at most once per `error_backoff`.
- Only 2xx with valid JSON is cached, so a maintenance page never replaces data. Upstream 4xx is passed through.
- The key includes the method, path, query, `Content-Type` (even with an empty body) and body. The order of different
  query parameters and equivalent URL encodings do not change it; the order of a repeated parameter's values does,
  and `flag` differs from `flag=`. The upstream receives the original query string. `Content-Type` is the only client
  header forwarded.

**Replicas.** With Redis, one replica fetches a key while the others wait for it in Redis; the lock is a 30 s lease
renewed during the fetch. Replica memory is reconciled with Redis in the background every `local_check`.

**Isolation.** A slow key does not delay others (`concurrency`), a slow source does not delay other sources (each has
its own `max_in_flight` slots). Exceptions: cold requests to a hung source can take all its slots, and its background
refresh waits (clients get STALE); a copy evicted by `local_max_bytes` is read from Redis synchronously.

**Limits.** A URL with `${__from}` is a new key on every dashboard refresh, and every key costs memory, Redis space and
an upstream request each `ttl`. So a source holds at most `max_keys` keys besides `warmup` (new keys above the limit are
fetched without storing, `BYPASS`), a key not requested for `idle_ttl` is deleted with its data, and copies in replica
memory are bounded by `local_max_bytes`. A source accepts at most `2 × max_in_flight` running and waiting fetches;
the rest get 503.

**Unstable network.** A broken or timed-out attempt is retried. A silently dead HTTP/2 connection is closed after an
unanswered ping in ~25 s; Go does not ping by default, and requests would hang on it for tens of minutes. A client that
gave up waiting is recorded as 499, not 5xx. A pod shutdown is not recorded as an upstream failure.

**Security.** The proxy calls upstreams with their keys, so `paths` is the boundary: a prefix matches whole segments
(`/cves` does not let into `/cves-admin`); paths with `.`/`..` (`%2e%2e` included), empty segments, `\` or control
characters get 400. Redirects are followed only within the scheme, host, base path, `paths` and `methods` (otherwise
502). Background requests stored in Redis are checked against the current configuration too.
Anyone who can reach the proxy uses your keys: keep it internal and limit who can call it (the chart has a
NetworkPolicy). Do not expose it through an Ingress.

Responses carry `X-Cache: HIT|STALE|MISS|BYPASS`, `Age` and `Last-Modified` (fetch time).

| Path | |
|---|---|
| `/<source>/…` | upstream data |
| `/-/entries` | JSON: keys, age, state, last error |
| `/metrics` | Prometheus / OpenMetrics |
| `/healthz` | liveness and readiness; does not depend on Redis or upstreams |

## Configuration

Sources and all settings live in YAML; nothing about particular sources is built in. `config.example.yml` lists every
key.

- File: `-config`, else `CONFIG_FILE`, else `./config.yml`; without a file — environment only.
- Precedence: defaults < file < environment.
- Unknown keys, invalid values and undefined variables fail the start, all at once, with line numbers.
- `${VAR}` in values comes from the environment; a bare `$` (`$filter` in OData) is left alone.
- `grafana-infinity-datasource-cache -check -config <file>` validates and prints the effective sources, secrets hidden.

```yaml
sources:
  nvd:                                  # name: the /nvd/… path and the NVD_ environment prefix
    url: https://services.nvd.nist.gov  # base URL, the request path is appended
    paths: [/rest/json/]                # allowed prefixes (whole segments), default /
    methods: [GET]                      # with POST the body is part of the cache key
    ttl: 2h                             # default cache.ttl
    timeout: 3m                         # per attempt, default upstream.timeout
    miss_wait: 170s                     # how long a cold request waits; default cache.miss_wait
    attempts: 2
    concurrency: 4                      # keys refreshed in parallel by one replica
    max_keys: 200                       # keys besides warmup; default cache.max_keys
    max_in_flight: 8                    # concurrent upstream requests; default upstream.max_in_flight
    rate: 25/30s                        # per replica; empty or 0 means unlimited
    validate: json                      # none for non-JSON responses
    max_body: 64MiB                     # default upstream.max_body
    auth:
      header: apiKey
      value_file: /etc/grafana-infinity-datasource-cache/nvd/api-key   # re-read on every request
      # value: ${NVD_API_KEY}
    warmup:                             # kept in the cache forever, GET only
      - "/rest/json/cves/2.0?virtualMatchString=cpe:2.3:a:golang:go&noRejected=&resultsPerPage=2000"
    # enabled: false
```

Durations use the Go format plus days (`7d`, `1d12h`); sizes are bytes or `KiB`/`MiB`/`GiB`.

**Environment** overrides the file:

| Variable | File key | Default |
|---|---|---|
| `LISTEN_ADDR` | `listen` | `:8080` |
| `LOG_LEVEL`, `LOG_FORMAT` | `log.level`, `log.format` | `info`, `text` |
| `REDIS_ADDR`, `REDIS_DB`, `REDIS_KEY_PREFIX` | `redis.*` | —, `0`, `datasource-cache:`; no address — in-memory store |
| `REDIS_PASSWORD`, `REDIS_PASSWORD_FILE` | `redis.password`, `redis.password_file` | — |
| `CACHE_TTL`, `CACHE_MAX_STALE`, `CACHE_IDLE_TTL` | `cache.*` | `1h`, `2d`, `24h` |
| `CACHE_ERROR_BACKOFF`, `CACHE_MISS_WAIT` | `cache.*` | `1m`, `50s` |
| `CACHE_REFRESH_INTERVAL`, `CACHE_LOCAL_CHECK` | `cache.*` | `1m`, `10s` |
| `CACHE_MAX_KEYS`, `CACHE_LOCAL_MAX_BYTES` | `cache.*` | `200` per source, `64MiB` per replica |
| `UPSTREAM_TIMEOUT`, `UPSTREAM_MAX_BODY`, `UPSTREAM_MAX_IN_FLIGHT` | `upstream.*`, source defaults | `2m`, `64MiB`, `8` |
| `METRICS_ENTRY_LIMIT` | `metrics.entry_limit` | `200` keys with their own series, `0` disables |
| `CACHE_WARMUP` | — | space-separated `/<source><path>`, added to the sources' `warmup` |
| `SOURCES` | — | when set, exactly these sources are enabled |
| `<NAME>_URL`, `_PATHS`, `_METHODS`, `_TTL`, `_TIMEOUT`, `_MISS_WAIT`, `_ATTEMPTS`, `_CONCURRENCY`, `_MAX_IN_FLIGHT`, `_MAX_KEYS`, `_RATE`, `_VALIDATE`, `_MAX_BODY`, `_AUTH_HEADER`, `_AUTH_VALUE`, `_AUTH_VALUE_FILE`, `_WARMUP` | `sources.<name>.*` | name in upper case, `-` → `_` |

A source can be defined by the environment alone: `SOURCES=osv OSV_URL=https://api.osv.dev OSV_METHODS=POST …`.

## Metrics

A ready Grafana dashboard is in [dashboards/](dashboards/README.md).

All are `datasource_cache_*` with labels from fixed sets; per-key series are capped by `METRICS_ENTRY_LIMIT`. Key state
is computed every `CACHE_REFRESH_INTERVAL` from the shared store and is the same on all replicas — aggregate it with
`max by (source)`; counters are per replica — use `sum`. Scrapes do not touch Redis.

| Group | Metrics |
|---|---|
| Clients | `requests_total{result,code}` (result: hit, stale, miss, bypass, error, canceled — the client gave up, code 499), `request_duration_seconds`, `requests_in_flight`, `served_age_seconds`, `response_bytes_total{encoding}`, `rejected_requests_total{reason}` (unknown_source, unsafe_path, not_allowed, body_too_large), `coalesced_requests_total` |
| Refreshes | `refreshes_total{trigger,result}` (trigger: miss, stale, scheduled; result: success, error, backoff, locked, current — another replica has just refreshed it), `refresh_duration_seconds`, `refreshes_in_flight`, `lock_waits_total{outcome}`, `lock_wait_duration_seconds`, `lock_lost_total`, `refresh_cycle_last_timestamp_seconds` |
| Keys | `entries{state}` (fresh, stale, missing), `entries_failing`, `entry_max_age_seconds`, `stored_bytes{encoding}`; per key: `entry_age_seconds`, `entry_size_bytes`, `entry_failing`, `entry_last_access_timestamp_seconds` |
| Upstreams | `upstream_requests_total{code}`, `upstream_errors_total{reason}` (timeout, eof, reset, refused, dns, tls, http_3xx — redirect outside the source, http_4xx, http_429, http_5xx, invalid_json, …), `upstream_retries_total`, `upstream_request_duration_seconds`, `upstream_requests_in_flight`, `upstream_rate_limit_wait_seconds`, `upstream_slot_wait_seconds`, `upstream_response_size_bytes`, `upstream_last_success_timestamp_seconds` |
| Replica memory and Redis | `lookups_total{result}` (local, loaded, absent, error), `revalidations_total{result}` (current, updated, loaded, kept, error), `local_entries`, `local_bytes`, `local_evictions_total`, `store_up`, `store_operation_duration_seconds{op}`, `store_errors_total{op}`, `redis_pool_*` |
| Configuration | `source_ttl_seconds`, `source_timeout_seconds`, `source_miss_wait_seconds`, `source_rate_limit_rps`, `source_concurrency`, `source_max_keys`, `source_max_in_flight`, `config_*_seconds`, `config_local_max_bytes`, `build_info{version,revision,goversion}` |

Plus `process_*`, `go_*` with extended runtime metrics (GC pauses, scheduler latency, memory classes) and
`promhttp_metric_handler_*`.

A key that never had data and gets 4xx from the upstream (a typo in a query) stops being tracked. A key with data that
gets 4xx stays and shows up as failing — that is what an expired API key looks like.

Thresholds come from the configuration metrics:

```promql
# Data older than two TTLs: the source is not being refreshed (alert)
max by (source) (datasource_cache_entry_max_age_seconds)
  > on (source) 2 * max by (source) (datasource_cache_source_ttl_seconds)

# A key clients ask for has no data at all (alert)
max by (source) (datasource_cache_entries{state="missing"}) > 0

# Availability SLI: proxy failures (502/504); 4xx is a client error, 499 a client that gave up
sum by (source) (rate(datasource_cache_requests_total{code=~"5.."}[5m]))
  / sum by (source) (rate(datasource_cache_requests_total[5m]))

# Share of responses served without waiting for the upstream
sum by (source) (rate(datasource_cache_requests_total{result=~"hit|stale"}[1h]))
  / sum by (source) (rate(datasource_cache_requests_total[1h]))

# Why an upstream fails (http_4xx are client query errors, not upstream ones)
sum by (source, reason) (increase(datasource_cache_upstream_errors_total{reason!="http_4xx"}[1h]))

# p99 age of served data: what dashboards actually show
histogram_quantile(0.99, sum by (le, source) (rate(datasource_cache_served_age_seconds_bucket[1h])))

# Background refresh of a source is stuck: the loop hangs or all slots are busy (alert)
time() - max by (source) (datasource_cache_refresh_cycle_last_timestamp_seconds)
  > 5 * scalar(max(datasource_cache_config_refresh_interval_seconds))

# Redis is down: replicas serve from their own memory
min(datasource_cache_store_up) == 0

# A source hit max_keys and new keys bypass the cache (alert; often ${__from} in a URL)
sum by (source) (rate(datasource_cache_requests_total{result="bypass"}[15m])) > 0

# Space used in Redis
sum(max by (source) (datasource_cache_stored_bytes{encoding="gzip"}))
```

## Development

Requires Go 1.27.4; debugging needs Delve 1.27 or newer.

```
make run         # in-memory store, sources from config.example.yml; your own: make run CONFIG=<file>
make redis       # Redis in Docker
make run-redis   # shared store; a second replica: LISTEN_ADDR=:8081 make run-redis
make test lint   # no Docker or network needed: in-memory HTTP, testing/synctest, miniredis
make chart-lint chart-test
```

- NVD key: `NVD_AUTH_HEADER=apiKey NVD_AUTH_VALUE=<key> NVD_RATE=50/30s make run`; without a key NVD allows
  5 requests per 30 s.
- Debugging: `dlv debug ./cmd/grafana-infinity-datasource-cache -- -config config.example.yml`. Useful breakpoints:
  `Cache.Get` (HIT/STALE/MISS), `Cache.refresh` (fetch under the lock), `Source.fetchOnce`.
- Upstream latency depends on where the proxy runs: tune timeouts by production metrics, not by local runs.
- [docs/features.md](docs/features.md) — the contract of every feature and how to verify it by hand.

## Releasing

`git tag v0.2.0 && git push origin v0.2.0` publishes the image `sakharovmaksim/grafana-infinity-datasource-cache`
with tags `0.2.0`, `0.2` and `latest`, the chart `0.2.0` to `oci://ghcr.io/sakharovmaksim/charts` and a GitHub release.
Every commit to `master` publishes the image tags `master` and `master-<sha>`.

## License

[MIT](LICENSE)
