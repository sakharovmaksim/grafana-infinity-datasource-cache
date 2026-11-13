# Features and how to verify them

For whoever checks the service after a change, people and agents alike. Every feature lists its contract (what must
hold), a manual check with the expected result, the tests that guard it and where it shows up in metrics. The design
and the full list of settings are in the [README](../README.md). All scenarios below were checked against the stub and
the real CVEDB and NVD.

## Environment

- Tests: `make test` (`go test -race ./...`) and `make lint`. No Docker or network: HTTP stubs are
  `httptest.NewTestServer` on an in-memory network (requests only through `srv.Client()`), Redis is miniredis. Time
  and concurrency tests run in `testing/synctest` bubbles on virtual time: they are deterministic and instant, and
  "finished within N ms" there means "did not wait for a single timer". Exceptions: the write deadline test (needs a
  real socket, `Start()` on loopback) and the miniredis tests (real TCP does not fit in a bubble).
- Service: `make run` — in-memory store; `make redis` + `make run-redis` — shared store; a second replica:
  `LISTEN_ADDR=:8081 make run-redis`.
- Errors, delays and data changes are checked against the stub below: real upstreams cannot be controlled.
- Quote URLs with `?` in zsh.

### Upstream stub

Save as `/tmp/stub.py` and run `python3 /tmp/stub.py`. The `t` field of a response is the stub's response time: it
shows whether the cached data was refreshed.

```python
# Fake upstream for grafana-infinity-datasource-cache: JSON with the response time in "t".
#   /status/<code>  that HTTP code      /slow?s=N  answer after N seconds
#   /html           200 with HTML       /drop      close the connection without a response
#   touch /tmp/stub-fail -> every path answers 503;  rm it to recover
import http.server, json, os, time, urllib.parse

class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(u.query)
        n = int(self.headers.get("Content-Length") or 0)
        body_in = self.rfile.read(n).decode() if n else ""
        if os.path.exists("/tmp/stub-fail"):
            return self.reply(503, b"fail")
        if u.path.startswith("/status/"):
            return self.reply(int(u.path.split("/")[2]), b'{"error":"stub"}')
        if u.path == "/slow":
            time.sleep(float(q.get("s", ["5"])[0]))
        if u.path == "/drop":
            self.close_connection = True
            return self.connection.close()
        if u.path == "/html":
            return self.reply(200, b"<html>maintenance</html>", "text/html")
        self.reply(200, json.dumps({"path": u.path, "query": u.query, "body": body_in, "t": time.time()}).encode())

    do_POST = do_GET

    def reply(self, code, body, ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

http.server.ThreadingHTTPServer(("127.0.0.1", 9999), H).serve_forever()
```

The service on the stub with short intervals:

```
SOURCES=stub STUB_URL=http://127.0.0.1:9999 STUB_METHODS="GET POST" STUB_ATTEMPTS=1 STUB_TIMEOUT=10s \
CACHE_TTL=20s CACHE_REFRESH_INTERVAL=5s CACHE_ERROR_BACKOFF=10s CACHE_MAX_STALE=1m CACHE_MISS_WAIT=3s \
LOG_LEVEL=debug go run ./cmd/grafana-infinity-datasource-cache

B=http://localhost:8080
h() { curl -s -o /dev/null -D - "$@" | grep -iE '^(HTTP|x-cache)' | tr -d '\r' | tr '\n' ' '; echo; }
```

## 1. Transparent proxy

- `/<source><path>?<query>` goes to `<source URL><path>?<query>`; the body and `Content-Type` are the upstream's.
- An unknown source, a path outside `paths` or a method outside `methods` → 404 with `{"error": …}`, nothing goes
  upstream. A `paths` prefix matches whole segments: `/cves` lets into `/cves` and `/cves/…`, not `/cves-admin`.
- A path with `.`/`..` segments in any encoding (`%2e%2e`, `..%2f`, `..;`), empty segments, `\` or control
  characters → 400: otherwise the source key would reach another path, normalized by the upstream itself.
- An upstream redirect is followed only within the same scheme and host; elsewhere it is an `http_3xx` error (502 to
  the client): Go carries a non-standard key header (`apiKey`) to any host, unlike `Authorization`.
- Client headers do not go upstream. The API key is added by the service (`auth.header` + `auth.value` or
  `auth.value_file`).
- HEAD is served as GET without a body, with the same cache key; HEAD never goes upstream.

Check:
- `h "$B/nope/x"` and `h -X DELETE "$B/stub/ok"` → 404.
- A real upstream: the response through the cache equals the direct one. NVD differs only in `timestamp`:
  ```
  diff <(curl -s "$B/cvedb/cves?product=go&limit=1000") <(curl -s --compressed "https://cvedb.shodan.io/cves?product=go&limit=1000")
  ```

Tests: `TestProxy`, `TestHeadServedAsGet`, `TestFetch`, `TestBuildDefaultsAndOverrides`,
`TestAllowsStaysInsidePrefix`, `TestUnsafePathNeverReachesUpstream`, `TestRedirectStaysOnOrigin`.
Metrics: `rejected_requests_total{reason="unknown_source|unsafe_path|not_allowed|body_too_large"}`.

## 2. Cache key

- The order of different parameter names and equivalent URL encodings (`%3A` = `:`) give one key. The order of a
  repeated parameter's values is kept; `flag` and `flag=` give different keys.
- The query string goes upstream unchanged: no sorting, no re-encoding.
- The body and `Content-Type` are part of the key; Content-Type is forwarded even with an empty body. A body over
  1 MiB → 413.
- The key format is versioned: a new version does not reuse old entries; warmup refills the cache and old keys expire
  by TTL/idle_ttl.

Check:
- `h "$B/stub/ok?b=2&a=1"` → MISS, `h "$B/stub/ok?a=1&b=2"` → HIT.
- `h "$B/stub/enc?x=a:b&flag"` → MISS, `h "$B/stub/enc?flag&x=a%3Ab"` → HIT; with `flag=` → MISS.
- `h -X POST -d '{"a":1}' "$B/stub/q"` twice → MISS, HIT; with `{"a":2}` → MISS.

Tests: `TestCanonicalQueryKeepsValuelessFlags`, `TestRequestKeyQuerySemantics`,
`TestRequestContentTypeWithAndWithoutBody`, `TestPostBodyIsPartOfKey`, `TestProxyKeepsRequestVariantsSeparate`,
`TestLegacyKeysAreNotServedOrRefreshed`.

## 3. HIT, STALE, MISS

- Younger than TTL → HIT. From TTL to `CACHE_MAX_STALE` → STALE and a background refresh. No data or older than max
  stale → MISS: a synchronous upstream request.
- Headers: `X-Cache`, `Age` (seconds since the fetch), `Last-Modified` (fetch time), `Vary: Accept-Encoding`.
  Infinity does not see headers; the state for dashboards is in `/-/entries`.
- While the upstream works, STALE is rare: the background refreshes keys before TTL (section 8).

Check: `h "$B/stub/ok"` → MISS, again → HIT. STALE is in section 4.

Tests: `TestMissThenHit`, `TestStaleServedAndRefreshedInBackground`.

## 4. Stale data during an outage and an explicit failure

- The upstream fails → clients get the last good copy (STALE), but not longer than `CACHE_MAX_STALE` since the fetch.
  Then 502 instead of arbitrarily old data.
- The upstream is back → the background refreshes the key after the `CACHE_ERROR_BACKOFF` pause.

Check (TTL 20 s, max stale 1 min):
1. `curl -s "$B/stub/ok"` — note `t`.
2. `touch /tmp/stub-fail` and wait 20 s: `h "$B/stub/ok"` → STALE with the same `t`; in `/-/entries` the key has
   `state: stale` and `error: upstream returned 503`.
3. Another ~40 s → 502 `{"error":"upstream failed recently: upstream returned 503: fail"}`.
4. `rm /tmp/stub-fail`, after 10–15 s → HIT with a fresh `t`.

Tests: `TestStaleKeptWhileUpstreamFails`, `TestMaxStaleExceededFailsExplicitly`, `TestOutageAndRecovery`.
Metrics: `entries{state="stale"}`, `entries_failing`, `entry_max_age_seconds`, `served_age_seconds`.

## 5. Upstream errors

- 4xx other than 429 → the same code to the client, not cached, no retries.
- 5xx, 429, network errors and timeouts → retry (`attempts`, 2 by default, 3 s apart); without data the client gets
  504 when the upstream did not answer in time (attempt timeout, no free slot, `miss_wait`), 502 otherwise. The code
  is kept during the pause after the failure. `Retry-After` is ignored.
- A silently dead HTTP/2 connection (packets lost, socket open) is closed after an unanswered ping: 15 s of silence +
  10 s for the answer; the retry uses a new connection. Go does not ping by default, and all requests to the host
  would hang on the dead connection until the kernel timeout. For HTTP/1.1 a dead connection costs one attempt up to
  `timeout`.
- A failure recorded after the fetch ran out of its time (it waited for a slot of a hung source) still reaches Redis:
  the write does not use the fetch context, otherwise it failed and opened the Redis breaker for all sources of the
  replica. Cancellation on shutdown is not a failure: other replicas must not pause.
- 2xx with invalid JSON (a maintenance page) is an error and is not cached. `validate: none` turns this off.
- A body larger than `max_body` (`upstream.max_body`) is an error.
- A cold request waits at most the source's `miss_wait` (default `CACHE_MISS_WAIT`, 50 s — below the Infinity timeout)
  → 504, while the fetch goes on and fills the cache. For feeds that take minutes raise `miss_wait` of that source,
  keeping it below the datasource timeout. A client that gave up earlier is counted as `result="canceled"`, code 499,
  logged at debug: it is not a service failure and must not get into an SLI over `5..`.

Check:
- `h "$B/stub/status/404"` → 404; `h "$B/stub/html"` → 502.
- `h "$B/stub/slow?s=5"` → 504 after 3 s; 3 s later again → HIT.
- `curl "$B/stub/drop"` → 502, `upstream_errors_total{reason="eof"}` in the metrics.

Tests: `TestClientErrorPassedThrough`, `TestInvalidJSONNotCached`, `TestUpstreamErrors`,
`TestDeadHTTP2ConnectionIsReplaced`, `TestHangingSourceDoesNotStallOthers`, `TestFetchDeadlineDoesNotBreakStore`,
`TestShutdownIsNotAnUpstreamFailure`, `TestMissWaitTimeoutKeepsFetching`, `TestFetch`, `TestUpstreamErrorMetrics`,
`TestNetworkReason`.
Metrics: `upstream_errors_total{reason}`, `upstream_retries_total`, `requests_total{result="error",code}`.

## 6. Pause after a failure

- After a failed fetch the key is not requested upstream again before `CACHE_ERROR_BACKOFF`. The pause is shared by
  all replicas (Redis, `fail:<key>`). Requests during the pause immediately get the stored error with the same code.

Check: `h "$B/stub/status/503"` twice in a row → both 502, one request in the stub log.

Tests: `TestColdMissFailureBacksOff`, `TestStaleKeptWhileUpstreamFails`, `TestRefreshTriggerMetrics`.
Metrics: `refreshes_total{result="backoff"}`.

## 7. Query typos are not tracked

- A key that never had data and gets 4xx from the upstream stops being tracked: it is not refreshed in the background
  and does not spoil `entry_max_age_seconds`. A key with data that gets 4xx stays and shows up as failing — that is
  what an expired API key looks like.

Check: `h "$B/stub/status/404"`; 5–10 s later the key is gone from `curl -s "$B/-/entries"`. The keys `/stub/html` and
`/stub/status/503` stay with `state: missing`: they are not 4xx.

Tests: `TestInvalidQueryIsNotTracked`.

## 8. Background refresh

- Tracked keys: those requested within `CACHE_IDLE_TTL`, plus the sources' `warmup` and `CACHE_WARMUP` (always). An
  access is recorded asynchronously, at most once a minute per key, so a key appears in `/-/entries` with a delay.
- Refresh ahead: as soon as age + `CACHE_REFRESH_INTERVAL` ≥ TTL, so clients get HIT.
- Every source has its own loop. Keys are refreshed in parallel, up to `concurrency` (4) per replica. A pass does not
  wait for running refreshes and skips a key still refreshing since the previous pass: a slow key takes one slot and
  delays neither other keys nor other sources.
- The upstream request limit is `rate`, per replica.
- Warmup is validated at start: an unknown source or a forbidden path fails the start. GET only.

Check: run with `CACHE_WARMUP='/stub/ok /stub/slow?s=40' STUB_TIMEOUT=60s`. `t` of `/stub/ok` changes every ~15 s
while `/stub/slow` hangs, and `refreshes_in_flight{source="stub"}` = 1.

Tests: `TestTickRefreshesAheadAndWarmsUp`, `TestIdleKeysStopRefreshing`, `TestSlowKeyDoesNotBlockRefresh`.
Metrics: `refreshes_total{trigger="scheduled"}`, `refreshes_in_flight`, `refresh_cycle_last_timestamp_seconds`.

## 9. Several replicas

- One key — one fetch for all replicas. The Redis lock is a 30 s lease that the holder renews every 10 s while the
  request runs.
- Concurrent cold requests in one replica coalesce into one fetch. A request in another replica waits for its result,
  polling Redis every 250 ms.
- Having taken the lock, a replica looks into Redis again: if another replica has already refreshed the key, the
  upstream is not called (`refreshes_total{result="current"}`).
- A crashed pod holds a key no longer than the lease.
- SIGTERM: 10 s for running requests, then fetches are cancelled and locks released (waiting up to 5 s).

Check (two replicas on `make redis`, ports 8080 and 8081):
- `curl -s "localhost:8080/stub/slow?s=3" & curl -s "localhost:8081/stub/slow?s=3" & wait` → one request in the stub
  log, `lock_waits_total{outcome="entry"}` on the second replica.
- During `curl "localhost:8080/stub/slow?s=30"` send the replica `kill -TERM`. The process exits as soon as client
  requests finish: a cold one by `CACHE_MISS_WAIT` (504), within 10 s. Without client requests — in a fraction of a
  second. After the exit `redis-cli -n 1 --scan --pattern 'datasource-cache:lock:*'` is empty.

Tests: `TestReplicasShareOneFetch`, `TestCoalescingAndLockWaitMetrics`, `TestRefreshReusesFreshStoreCopy`,
`TestDeadReplicaLockExpires`, `TestHeartbeatKeepsLockDuringLongFetch`, `TestSlowKeyDoesNotBlockRefresh` (Close).
Metrics: `coalesced_requests_total`, `lock_waits_total{outcome}`, `lock_wait_duration_seconds`, `lock_lost_total`.

## 10. Replica memory and Redis

- HIT and STALE are served from replica memory without touching Redis or the upstream. Reconciliation with Redis runs
  in the background at most once per `CACHE_LOCAL_CHECK`; an older copy never replaces a newer one.
- A replica without its own copy reads Redis synchronously, in one HMGET.
- A pod restart: data comes from Redis, the upstream is not called.
- Redis is unavailable:
  - replicas serve from memory;
  - after a Redis error, requests skip it for 5 s, the background tries on every pass;
  - `/-/entries` returns 503;
  - the service starts without Redis with a warning.
- Without `REDIS_ADDR` the store is in process memory. In Redis: database `REDIS_DB`, keys prefixed with
  `REDIS_KEY_PREFIX`, bodies gzipped.
- Replica copies are bounded by `CACHE_LOCAL_MAX_BYTES`: the least recently read are evicted and read from Redis on
  the next request (`lookups_total{result="loaded"}`). Without Redis an evicted key is a MISS again.
- A source holds at most `max_keys` keys besides `warmup`. A new key above the limit is served straight from the
  upstream (`X-Cache: BYPASS`), neither stored nor tracked; identical requests share the fetch. The count is updated by
  every background pass, so within one pass replicas together may exceed the limit by the keys they accepted. A key
  idle for `CACHE_IDLE_TTL` frees its place and is deleted from Redis with its data: otherwise data would live another
  `CACHE_MAX_STALE` after the last refresh, and a stream of one-off keys would take ~3× the limit. The price: such a
  key requested again is a MISS, not STALE.
- Upstream fetches are bounded by the source's `max_in_flight` (`UPSTREAM_MAX_IN_FLIGHT`) per replica, the rest wait
  for a slot. Every source has its own slots: a shared pool let a hung source take them all and stop the others.

Check:
- Request a key, restart the replica, request again → HIT, no `upstream_requests_total` in `/metrics`.
- Stop Redis → responses stay HIT with the same latency, `store_up` = 0, the `store unavailable` warning in the log at
  most once per 10 s.

Tests: `TestHitDoesNotWaitForStore`, `TestRedisOutageServesLocalCopy`, `TestLookupMetrics`, `TestStores`,
`TestRedisMetrics`, `TestMaxKeysBypassesNewKeys`, `TestLocalCopiesStayWithinLimit`, `TestSlotsBoundUpstreamRequests`.
Metrics: `lookups_total`, `revalidations_total`, `store_up`, `store_errors_total{op}`,
`store_operation_duration_seconds{op}`, `local_entries`, `local_bytes`, `local_evictions_total`, `redis_pool_*`,
`requests_total{result="bypass"}`, `upstream_slot_wait_seconds`, `source_max_in_flight`.

## 11. Responses

- A client with `Accept-Encoding: gzip` gets the stored gzip body as is. Without gzip the body is decompressed as a
  stream, `Content-Length` is the original size.
- Write deadline: 1 min after the data is ready, so a client that does not read the response is disconnected. The
  deadline resets between keep-alive requests.
- `ReadHeaderTimeout` 10 s, `ReadTimeout` 30 s, `IdleTimeout` 2 min.

Check: `curl -sI -H 'Accept-Encoding: gzip' "$B/stub/ok"` → `Content-Encoding: gzip`, `Content-Length` is the gzipped
size; without the header there is no `Content-Encoding` and `Content-Length` is the original size. Small bodies may
grow when gzipped — that is fine.

Tests: `TestProxy`, `TestWriteDeadlineDoesNotLeakAcrossKeepAlive`, `TestHeadServedAsGet`.
Metrics: `response_bytes_total{encoding}`, `request_duration_seconds{result}`, `requests_in_flight`.

## 12. Sources and configuration

- Nothing about particular sources is in the code: URLs, paths, limits and keys come only from the config
  (`config.example.yml`) or the environment. Order: defaults < YAML file < environment.
- File: `-config`, else `CONFIG_FILE`, else `./config.yml`. An unknown key, an invalid value or an undefined `${VAR}`
  fails the start with the line number of the source file; all errors are printed at once.
- `${VAR}` is substituted only in values (not in comments); a bare `$` (`$filter`) is left alone.
- `SOURCES`, when set, is exactly the list of enabled sources; a source can be defined by `<NAME>_*` alone.
- `auth.value_file` is re-read on every request. A missing file at start fails the start.
- `-check` prints the effective sources with defaults and exits; secrets are hidden.

Check (exit 1 and a clear message):
- a file with the typo `timout:` → `line N: unknown key sources.<name>.timout`;
- `auth: {value: "${NOPE}"}` → `undefined variables: NOPE`;
- no file and no `SOURCES` → `no sources: …`;
- `grafana-infinity-datasource-cache -check -config config.example.yml` → the effective cvedb and nvd.

Tests: `internal/config/config_test.go` (precedence, strict keys with line numbers, variables, `SOURCES`, validation,
warmup), `TestBuildDefaultsAndOverrides`, `TestBuildReportsBadRates`, `TestFetch` (key rotation),
`TestParseDuration`, `TestParseTarget`.

## 13. Status and metrics

- `/healthz` — always 200, independent of Redis and upstreams.
- `/-/entries` — JSON for tracked keys: `request`, `fetched_at`, `age_seconds`, `state` (fresh, stale, missing),
  `size`, `stored_size`, `last_access`, `error`, `error_at`.
- `/metrics` — Prometheus or OpenMetrics. Collection does not touch Redis, and a failing collector does not break the
  whole response. The metric list and PromQL are in the README.
- Per-key series are capped by `METRICS_ENTRY_LIMIT`, oldest first; `0` disables them.
- The configuration is exported as metrics (`source_ttl_seconds`, `config_*_seconds`, …), the version as
  `build_info{version,revision,goversion}`.

Tests: `TestMetricsEndpoint` (runs promlint), `TestEntryCollector`, `TestRefreshTriggerMetrics`,
`TestUpstreamErrorMetrics`.

## 14. Resource usage

Measured on an Apple M4 with `GOMAXPROCS=2` and `GOMEMLIMIT=200MiB`. Server CPUs may be slower: take the orders of
magnitude, not the exact numbers.

| Scenario | CPU | Memory |
|---|---|---|
| Idle after warming up 3 keys (0.6 + 5.2 + 1.8 MB) | ~0 | RSS 36 MiB, heap 5–14 MiB |
| HIT with gzip, a 5.2 MB NVD feed, 20 clients | ~51 µs per response (24k rps on 2 cores), p99 2.4 ms | RSS 36 MiB, no growth |
| HIT without gzip (decompression on every response) | ~5.6 ms per response, p99 130 ms | RSS 36 MiB |
| Fetching a 63 MiB response (close to `UPSTREAM_MAX_BODY`) | ~0.3 s | RSS 28 MiB |
| 4 / 8 such fetches in parallel | 1.2 / 2.4 s | RSS 55 / 104 MiB, ~4 MiB per key of it are the copies |
| 200 keys of 6.2 MB | ~30 ms per fetch | copies 79 MiB (~0.4 MB per key), heap up to 2× the copies before GC |

- An upstream response is processed as a stream (JSON validation, gzip and sha256 in one pass), so a fetch costs
  about the size of its gzipped copy. Before that, four 63 MiB feeds took 511 MiB RSS — an OOM under a 256 MiB limit.
- Memory grows linearly with the number of keys: the gzipped copy of each lives in replica memory and in Redis. A 6 MB
  NVD feed is about 0.4 MB in both.
- How the default limits fit a 256 MiB pod with `GOMEMLIMIT` 200 MiB: idle ~36 MiB, `local_max_bytes` copies 64 MiB,
  fetches `max_in_flight` 8 per source × 2 sources × up to ~4 MiB per large feed = 64 MiB; ~165 MiB of live memory in
  total. Every additional source with large responses adds up to 32 MiB. In Redis, `max_keys` 200 is up to
  200 × 0.4 MB = 80 MB per source with NVD-sized feeds (data leaves Redis with its key after `CACHE_IDLE_TTL`).
- To repeat: a sampler reads `process_cpu_seconds_total`, `process_resident_memory_bytes` and
  `go_memstats_heap_inuse_bytes` from `/metrics` under load. The load client must be Go with keep-alive and
  `Accept-Encoding: gzip`, like Grafana; `ab` does not fit.

## Before a release

1. `make test`, `make lint`, `make chart-lint chart-test`.
2. Real upstreams: `make run` (CVEDB and NVD from `config.example.yml`). The response through the cache equals the
   direct one (NVD without `timestamp`), a repeated request is a HIT.
3. Two replicas on Redis: `upstream_requests_total` summed over replicas is one request per key.
4. After deploying:
   - a request from the Grafana pod network to the full service URL → 200;
   - `/-/entries` (`kubectl port-forward`, see the chart notes) shows the warmup keys;
   - `datasource_cache_build_info` is scraped.

## Pitfalls

- Your machine may reach the internet by another route than production: upstream speed and errors locally are not
  representative. Large NVD feeds can take minutes.
- NVD without a key allows 5 requests per 30 s; frequent manual tests hit that (`NVD_AUTH_VALUE=<key>`).
- Shodan CVEDB sometimes drops the connection — expected and absorbed by the retry; visible as
  `upstream_errors_total{reason="eof"}`.
- A cold request to a key being refreshed in the background by the same replica waits for that fetch (up to
  `CACHE_MISS_WAIT`) instead of starting its own.
- Debugging Go 1.27 needs Delve 1.27 or newer. go-redis logs at debug level.
