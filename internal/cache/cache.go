// Package cache implements a read-through cache with stale-while-revalidate,
// background refresh of recently used keys and cross-replica coordination through a Store.
//
// Nothing a client waits for depends on the upstream or the store unless there is no data at all:
// hits and stale hits are served from replica memory, revalidation and refresh run in the background.
package cache

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/singleflight"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/metrics"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/source"
)

type Status string

const (
	Hit   Status = "HIT"
	Stale Status = "STALE"
	Miss  Status = "MISS"
	// Bypass: a new key over the source's max_keys, fetched for this request only.
	Bypass Status = "BYPASS"
)

// ErrTimeout: a cold request waited CACHE_MISS_WAIT; the fetch goes on and fills the cache.
var ErrTimeout = errors.New("upstream is slow, the response is still being fetched; retry later")

var (
	errLocked  = errors.New("another replica is fetching")
	errRetry   = errors.New("fetch lock released without a result")
	errBackoff = errors.New("upstream failed recently")
	errClosed  = errors.New("cache is shutting down")
)

// Refresh triggers, the trigger label of refreshes_total.
const (
	trigMiss      = "miss"
	trigStale     = "stale"
	trigScheduled = "scheduled"
)

const (
	trackEvery   = time.Minute
	pollEvery    = 250 * time.Millisecond
	storeTimeout = 3 * time.Second
	fetchMargin  = 30 * time.Second
	// After a store error client requests skip the store for this long: with Redis down every
	// call would otherwise add the client's dial retries (seconds) to the request.
	storeBreaker = 5 * time.Second
	// The fetch lock is a short lease renewed while the fetch runs: a replica that dies mid-fetch
	// blocks the key for others for at most one lease, not for the whole upstream timeout.
	defaultLease = 30 * time.Second
)

type Options struct {
	MaxStale        time.Duration
	IdleTTL         time.Duration
	RefreshInterval time.Duration
	ErrorBackoff    time.Duration
	MissWait        time.Duration
	LocalCheck      time.Duration
	LocalMaxBytes   int64         // replica copies, 0 = unlimited
	LockLease       time.Duration // 0 = 30 s
	Warmup          []source.Request
	Owner           string // lock owner, unique per replica
	EntryMetrics    int    // max per-entry series, 0 = none
}

type Cache struct {
	store   Store
	sources map[string]*source.Source
	opt     Options
	log     *slog.Logger
	now     func() time.Time
	sf      singleflight.Group

	ctx    context.Context // parent of all background work, canceled by Close
	cancel context.CancelFunc
	bg     sync.WaitGroup

	mu       sync.Mutex
	closed   bool
	local    map[string]*localEntry // replica copy: survives Redis outages and saves transfers
	localLen int64                  // gzipped bytes in local
	warm     map[string]bool        // warmup keys: always admitted, not counted against max_keys
	failures map[string]*Failure    // replica backoff for when Redis is unreachable
	touched  map[string]time.Time
	fetching map[string]bool         // cold fetches running in this replica
	state    map[string]*sourceState // background refresh, per source
	lastWarn time.Time
	downTill time.Time // store breaker, see storeBreaker
}

type localEntry struct {
	e       *Entry
	checked time.Time
	read    time.Time // last client read, for eviction
}

type entryStat struct {
	source, request string
	age             time.Duration // < 0: no data
	size, stored    int64
	lastAccess      time.Time
	failing         bool
}

// sourceState is guarded by Cache.mu except sem and wg.
type sourceState struct {
	sem      chan struct{}        // <NAME>_CONCURRENCY slots
	wg       sync.WaitGroup       // running key refreshes, waited for by tick
	inflight map[string]bool      // keys being refreshed: a pass skips them instead of waiting
	stats    map[string]entryStat // last result per tracked key
	// keys counts tracked keys besides warmup: set by every pass, plus keys admitted since.
	keys     int
	admitted map[string]bool
	bypassed int // since the last pass, for its log line
}

func New(store Store, sources map[string]*source.Source, opt Options, log *slog.Logger) *Cache {
	if opt.LockLease <= 0 {
		opt.LockLease = defaultLease
	}
	ctx, cancel := context.WithCancel(context.Background())
	state := make(map[string]*sourceState, len(sources))
	for name, s := range sources {
		state[name] = &sourceState{
			sem:      make(chan struct{}, max(s.Concurrency, 1)),
			inflight: map[string]bool{},
			stats:    map[string]entryStat{},
			admitted: map[string]bool{},
		}
	}
	warm := make(map[string]bool, len(opt.Warmup))
	for _, r := range opt.Warmup {
		warm[r.Key()] = true
	}
	return &Cache{
		store:    Instrument(store),
		sources:  sources,
		opt:      opt,
		log:      log,
		now:      time.Now,
		ctx:      ctx,
		cancel:   cancel,
		local:    map[string]*localEntry{},
		warm:     warm,
		failures: map[string]*Failure{},
		touched:  map[string]time.Time{},
		fetching: map[string]bool{},
		state:    state,
	}
}

// Close aborts background fetches and waits up to timeout for them to release their locks,
// so other replicas take over the keys at once instead of after a lease.
func (c *Cache) Close(timeout time.Duration) {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.cancel()
	done := make(chan struct{})
	go func() { c.bg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		c.log.Warn("background work did not stop in time", "timeout", timeout)
	}
}

// enter registers background work; it fails after Close so the WaitGroup is never reused.
func (c *Cache) enter() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.bg.Add(1)
	return true
}

// flight runs fn once per key in this replica, detached from the caller and bounded by timeout.
func (c *Cache) flight(key string, timeout time.Duration, fn func(ctx context.Context) (any, error)) <-chan singleflight.Result {
	return c.sf.DoChan(key, func() (any, error) {
		if !c.enter() {
			return nil, errClosed
		}
		defer c.bg.Done()
		ctx, cancel := context.WithTimeout(c.ctx, timeout)
		defer cancel()
		return fn(ctx)
	})
}

// Get serves fresh data as is, stale data while refreshing it in the background,
// and fetches synchronously only when there is nothing within CACHE_MAX_STALE.
func (c *Cache) Get(ctx context.Context, r source.Request) (*Entry, Status, error) {
	src := c.sources[r.Source]
	key := r.Key()
	e := c.lookup(ctx, key)
	if e == nil && !c.admit(src, key) {
		e, err := c.bypass(ctx, key, r)
		return e, Bypass, err
	}
	c.track(key, r)
	if e != nil {
		age := c.now().Sub(e.Meta.FetchedAt)
		switch {
		case age < src.TTL:
			return e, Hit, nil
		case age < c.opt.MaxStale:
			c.refreshAsync(key, r)
			return e, Stale, nil
		}
	}
	e, err := c.fetchWait(ctx, key, r)
	return e, Miss, err
}

// admit decides whether a key without data becomes one more key of its source. Every key costs memory
// in each replica and in the Redis shared with Grafana, and an upstream request per TTL for CACHE_IDLE_TTL,
// so a source holds at most max_keys besides warmup. Replicas may overshoot by what they admit between passes.
func (c *Cache) admit(src *source.Source, key string) bool {
	if src.MaxKeys <= 0 || c.warm[key] {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	st := c.state[src.Name]
	_, touched := c.touched[key]
	_, tracked := st.stats[key]
	switch {
	case touched || tracked || st.admitted[key]:
		return true
	case st.keys >= src.MaxKeys:
		st.bypassed++
		return false
	}
	st.keys++
	st.admitted[key] = true
	return true
}

// bypass fetches a key over max_keys for this request only: nothing is stored or tracked.
// Identical requests in flight share one fetch.
func (c *Cache) bypass(ctx context.Context, key string, r source.Request) (*Entry, error) {
	ch := c.flight("by:"+key, c.fetchTimeout(r), func(ctx context.Context) (any, error) {
		resp, err := c.sources[r.Source].Fetch(ctx, r)
		if err != nil {
			c.logResult(r, err)
			return nil, err
		}
		return newEntry(r, resp, c.now()), nil
	})
	return c.wait(ctx, ch, r)
}

// wait returns the fetch result, or gives up after the source's miss_wait while the fetch goes on.
func (c *Cache) wait(ctx context.Context, ch <-chan singleflight.Result, r source.Request) (*Entry, error) {
	d := c.opt.MissWait
	if s := c.sources[r.Source]; s != nil && s.MissWait > 0 {
		d = s.MissWait
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case res := <-ch:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Val.(*Entry), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, ErrTimeout
	}
}

func (c *Cache) lookup(ctx context.Context, key string) *Entry {
	e, result := c.lookupResult(ctx, key)
	metrics.Lookups.WithLabelValues(result).Inc()
	return e
}

// lookupResult answers from replica memory without waiting for the store; checking the store for a
// newer copy runs in the background once per CACHE_LOCAL_CHECK. Only a replica without a copy reads
// the store synchronously.
func (c *Cache) lookupResult(ctx context.Context, key string) (*Entry, string) {
	c.mu.Lock()
	l := c.local[key]
	due := l != nil && c.now().Sub(l.checked) >= c.opt.LocalCheck
	if due {
		l.checked = c.now()
	}
	if l != nil {
		l.read = c.now()
	}
	c.mu.Unlock()
	if l != nil {
		if due && c.storeAvailable() {
			c.revalidateAsync(key, l.e)
		}
		return l.e, "local"
	}
	if !c.storeAvailable() {
		return nil, "error"
	}
	ctx, cancel := context.WithTimeout(ctx, storeTimeout)
	defer cancel()
	e, err := c.store.Get(ctx, key)
	if err != nil {
		c.storeErr("get", err)
		return nil, "error"
	}
	if e == nil {
		return nil, "absent"
	}
	c.setLocal(key, e)
	return e, "loaded"
}

func (c *Cache) revalidateAsync(key string, mine *Entry) {
	c.flight("rv:"+key, 2*storeTimeout, func(ctx context.Context) (any, error) {
		metrics.Revalidations.WithLabelValues(c.revalidate(ctx, key, mine)).Inc()
		return nil, nil
	})
}

func (c *Cache) revalidate(ctx context.Context, key string, mine *Entry) string {
	m, err := c.store.Meta(ctx, key)
	switch {
	case err != nil:
		c.storeErr("meta", err)
		return "error"
	case m == nil || m.FetchedAt.Before(mine.Meta.FetchedAt):
		return "kept" // Redis lost the key or holds an older copy written before an outage
	case m.FetchedAt.Equal(mine.Meta.FetchedAt):
		return "current"
	case m.Hash == mine.Meta.Hash:
		// Another replica refreshed and got the same body: take the new meta, skip the transfer.
		c.setLocal(key, &Entry{Meta: *m, Gzip: mine.Gzip})
		return "updated"
	}
	e, err := c.store.Get(ctx, key)
	if err != nil {
		c.storeErr("get", err)
		return "error"
	}
	if e == nil {
		return "kept"
	}
	c.setLocal(key, e)
	return "loaded"
}

// setLocal never replaces a copy with an older one: background revalidation may race a refresh.
func (c *Cache) setLocal(key string, e *Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	read := c.now()
	if l := c.local[key]; l != nil {
		if e.Meta.FetchedAt.Before(l.e.Meta.FetchedAt) {
			return
		}
		read = l.read
		c.dropLocal(key)
	}
	c.local[key] = &localEntry{e: e, checked: c.now(), read: read}
	c.localLen += int64(len(e.Gzip))
	c.evict(key)
}

// evict drops the least recently read copies, except keep, until the replica fits CACHE_LOCAL_MAX_BYTES.
// They stay in the store and come back on the next read. Called with c.mu held.
func (c *Cache) evict(keep string) {
	if c.opt.LocalMaxBytes <= 0 || c.localLen <= c.opt.LocalMaxBytes {
		return
	}
	keys := make([]string, 0, len(c.local))
	for k := range c.local {
		if k != keep {
			keys = append(keys, k)
		}
	}
	sort.Slice(keys, func(i, j int) bool { return c.local[keys[i]].read.Before(c.local[keys[j]].read) })
	for _, k := range keys {
		if c.localLen <= c.opt.LocalMaxBytes {
			return
		}
		c.dropLocal(k)
		metrics.LocalEvictions.Inc()
	}
}

// dropLocal is called with c.mu held.
func (c *Cache) dropLocal(key string) {
	if l := c.local[key]; l != nil {
		c.localLen -= int64(len(l.e.Gzip))
		delete(c.local, key)
	}
}

func (c *Cache) fetchTimeout(r source.Request) time.Duration {
	return c.sources[r.Source].MaxFetchTime() + fetchMargin
}

func (c *Cache) refreshAsync(key string, r source.Request) {
	// The result channel is buffered, so nobody has to read it.
	c.flight("bg:"+key, c.fetchTimeout(r), func(ctx context.Context) (any, error) {
		_, err := c.refresh(ctx, key, r, trigStale)
		c.logResult(r, err)
		return nil, nil
	})
}

// fetchWait runs one fetch per key per replica; the fetch outlives impatient clients
// so a slow upstream still ends up in the cache.
func (c *Cache) fetchWait(ctx context.Context, key string, r source.Request) (*Entry, error) {
	c.mu.Lock()
	if c.fetching[key] {
		metrics.Coalesced.WithLabelValues(r.Source).Inc()
	}
	c.mu.Unlock()
	ch := c.flight("miss:"+key, c.fetchTimeout(r), func(ctx context.Context) (any, error) {
		c.setFetching(key, true)
		defer c.setFetching(key, false)
		for {
			e, err := c.refresh(ctx, key, r, trigMiss)
			if errors.Is(err, errLocked) {
				if e, err = c.waitOther(ctx, key, r); errors.Is(err, errRetry) {
					continue
				}
			}
			c.logResult(r, err)
			return e, err
		}
	})
	return c.wait(ctx, ch, r)
}

func (c *Cache) setFetching(key string, on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if on {
		c.fetching[key] = true
	} else {
		delete(c.fetching, key)
	}
}

// refresh fetches under the shared lock and stores the result.
func (c *Cache) refresh(ctx context.Context, key string, r source.Request, trigger string) (*Entry, error) {
	result := "error"
	defer func() { metrics.Refreshes.WithLabelValues(r.Source, trigger, result).Inc() }()

	if f := c.failure(ctx, key); f != nil {
		result = "backoff"
		err := fmt.Errorf("%w: %w", errBackoff, f.Err())
		c.forgetInvalid(ctx, key, err)
		return nil, err
	}
	src := c.sources[r.Source]
	ok, err := c.store.Lock(ctx, key, c.opt.Owner, c.opt.LockLease)
	held := err == nil && ok
	if err != nil {
		c.storeErr("lock", err)
		ok = true // without Redis each replica fetches for itself
	}
	if !ok {
		result = "locked"
		return nil, errLocked
	}
	if held {
		defer func() {
			uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
			defer cancel()
			if err := c.store.Unlock(uctx, key, c.opt.Owner); err != nil {
				c.storeErr("unlock", err)
			}
		}()
		defer c.heartbeat(ctx, key, r)()

		// Another replica may have finished this key between our check and the lock.
		if e := c.currentInStore(ctx, key, src, trigger); e != nil {
			c.setLocal(key, e)
			result = "current"
			return e, nil
		}
	}
	start := time.Now()
	defer func() {
		metrics.RefreshDuration.WithLabelValues(r.Source, trigger).Observe(time.Since(start).Seconds())
	}()

	resp, err := src.Fetch(ctx, r)
	// The writes below outlive ctx: a fetch of a hanging source may use up its deadline waiting for a slot,
	// and a write on a dead context would fail, lose the result for other replicas and trip the store
	// breaker for every source of this replica.
	wctx, wcancel := context.WithTimeout(context.WithoutCancel(ctx), storeTimeout)
	defer wcancel()
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, err // shutdown: says nothing about the upstream, other replicas must not back off
		}
		f := &Failure{Msg: err.Error(), At: c.now()}
		if se, ok := errors.AsType[*source.Error](err); ok {
			f.Status, f.Reason = se.Status, se.Reason
		}
		c.setFailure(wctx, key, f)
		c.forgetInvalid(wctx, key, err)
		return nil, err
	}
	e := newEntry(r, resp, c.now())
	if err := c.store.Put(wctx, key, e, c.opt.MaxStale); err != nil {
		c.storeErr("put", err)
	}
	c.setLocal(key, e)
	c.mu.Lock()
	delete(c.failures, key)
	c.mu.Unlock()
	metrics.UpstreamLastSuccess.WithLabelValues(r.Source).SetToCurrentTime()
	result = "success"
	c.log.Info("fetched", "request", r.String(), "trigger", trigger, "size", e.Meta.Size,
		"stored", e.Meta.StoredSize, "duration", time.Since(start).Round(time.Millisecond))
	return e, nil
}

// currentInStore returns the stored entry if it no longer needs this refresh. Scheduled refreshes
// run ahead of expiry, so for them "current" means it will not expire before the next pass.
func (c *Cache) currentInStore(ctx context.Context, key string, src *source.Source, trigger string) *Entry {
	m, err := c.store.Meta(ctx, key)
	if err != nil || m == nil {
		return nil
	}
	age := c.now().Sub(m.FetchedAt)
	if trigger == trigScheduled {
		age += c.opt.RefreshInterval
	}
	if age >= src.TTL {
		return nil
	}
	e, err := c.store.Get(ctx, key)
	if err != nil {
		return nil
	}
	return e
}

// heartbeat renews the lock lease until the returned stop is called.
func (c *Cache) heartbeat(ctx context.Context, key string, r source.Request) (stop func()) {
	hctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(c.opt.LockLease / 3)
		defer t.Stop()
		for {
			select {
			case <-hctx.Done():
				return
			case <-t.C:
			}
			ectx, ecancel := context.WithTimeout(hctx, storeTimeout)
			ok, err := c.store.Extend(ectx, key, c.opt.Owner, c.opt.LockLease)
			ecancel()
			switch {
			case err != nil:
				c.storeErr("extend", err)
			case !ok:
				// Lease expired (store outage, long GC): another replica may fetch the same key; harmless.
				metrics.LockLost.WithLabelValues(r.Source).Inc()
				c.log.Warn("fetch lock lost", "request", r.String())
				return
			}
		}
	}()
	return func() { cancel(); <-done }
}

// forgetInvalid stops tracking a key that never had data and gets a 4xx: a mistyped client query
// must not keep being refreshed or show up as a stale source. Keys with data stay tracked, so an
// expired API key is still visible as failing.
func (c *Cache) forgetInvalid(ctx context.Context, key string, err error) {
	if _, ok := source.ClientError(err); !ok || c.meta(ctx, key) != nil {
		return
	}
	if err := c.store.Forget(ctx, key); err != nil {
		c.storeErr("forget", err)
	}
}

func (c *Cache) waitOther(ctx context.Context, key string, r source.Request) (*Entry, error) {
	outcome := "timeout"
	start := time.Now()
	defer func() {
		metrics.LockWaits.WithLabelValues(r.Source, outcome).Inc()
		metrics.LockWaitDuration.WithLabelValues(r.Source).Observe(time.Since(start).Seconds())
	}()
	t := time.NewTicker(pollEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-t.C:
		}
		if e, err := c.store.Get(ctx, key); err == nil && e != nil {
			c.setLocal(key, e)
			outcome = "entry"
			return e, nil
		}
		if f, err := c.store.Failure(ctx, key); err == nil && f != nil {
			outcome = "failure"
			return nil, f.Err()
		}
		if locked, err := c.store.Locked(ctx, key); err == nil && !locked {
			outcome = "released"
			return nil, errRetry
		}
	}
}

func (c *Cache) failure(ctx context.Context, key string) *Failure {
	c.mu.Lock()
	f := c.failures[key]
	c.mu.Unlock()
	if f != nil && c.now().Sub(f.At) < c.opt.ErrorBackoff {
		return f
	}
	f, err := c.store.Failure(ctx, key)
	if err != nil {
		c.storeErr("failure", err)
		return nil
	}
	return f
}

func (c *Cache) setFailure(ctx context.Context, key string, f *Failure) {
	c.mu.Lock()
	c.failures[key] = f
	c.mu.Unlock()
	if err := c.store.SetFailure(ctx, key, f, c.opt.ErrorBackoff); err != nil {
		c.storeErr("set_failure", err)
	}
}

// track is throttled and asynchronous: it must not slow down or fail a client request.
func (c *Cache) track(key string, r source.Request) {
	now := c.now()
	c.mu.Lock()
	if now.Sub(c.touched[key]) < trackEvery {
		c.mu.Unlock()
		return
	}
	c.touched[key] = now
	c.mu.Unlock()
	if !c.storeAvailable() || !c.enter() {
		return
	}
	go func() {
		defer c.bg.Done()
		ctx, cancel := context.WithTimeout(c.ctx, storeTimeout)
		defer cancel()
		if err := c.store.Track(ctx, key, r, now); err != nil {
			c.storeErr("track", err)
		}
	}()
}

// Run refreshes tracked keys until ctx is done or Close is called. Every source has its own loop
// and worker pool, so a slow upstream delays neither other sources nor other keys of its own.
// Every replica runs it; the lock picks one fetcher per key.
func (c *Cache) Run(ctx context.Context) {
	if !c.enter() {
		return
	}
	defer c.bg.Done()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(c.ctx, cancel)()

	c.housekeeping(ctx) // warmup keys must be tracked before the first pass
	var wg sync.WaitGroup
	wg.Go(func() { every(ctx, c.opt.RefreshInterval, c.housekeeping) })
	for name := range c.sources {
		wg.Go(func() {
			c.refreshSource(ctx, name)
			every(ctx, c.opt.RefreshInterval, func(ctx context.Context) { c.refreshSource(ctx, name) })
		})
	}
	wg.Wait()
}

func every(ctx context.Context, d time.Duration, fn func(context.Context)) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fn(ctx)
		}
	}
}

// tick is one full pass for tests: housekeeping, all sources, and waiting for the started refreshes.
func (c *Cache) tick(ctx context.Context) {
	c.housekeeping(ctx)
	for name := range c.sources {
		c.refreshSource(ctx, name)
	}
	for name := range c.sources {
		c.state[name].wg.Wait()
		c.publish(name)
	}
}

func (c *Cache) housekeeping(ctx context.Context) {
	now := c.now()
	pctx, cancel := context.WithTimeout(ctx, storeTimeout)
	err := c.store.Ping(pctx)
	cancel()
	metrics.StoreUp.Set(b2f(err == nil))
	for _, r := range c.opt.Warmup {
		if err := c.store.Track(ctx, r.Key(), r, now); err != nil {
			c.storeErr("track", err)
		}
	}
	if err := c.store.Untrack(ctx, now.Add(-c.opt.IdleTTL)); err != nil {
		c.storeErr("untrack", err)
	}
	c.prune(now)
}

func (c *Cache) trackedFor(ctx context.Context, name string) []Tracked {
	now := c.now()
	all, err := c.store.Tracked(ctx, now.Add(-c.opt.IdleTTL))
	if err != nil {
		c.storeErr("tracked", err)
		all = c.localTracked(now)
	}
	out := all[:0]
	for _, t := range all {
		// Ignore previous key formats during a rolling upgrade. Do not delete
		// them: old replicas may still use them; idle TTL will clean them up.
		if t.Request.Source == name && t.Key == t.Request.Key() && c.sources[name].Allows(t.Request.Method, t.Request.Path) {
			out = append(out, t)
		}
	}
	return out
}

// refreshSource starts refreshes of the due keys, <NAME>_CONCURRENCY at a time, and does not wait for
// them to finish: a key still being fetched since an earlier pass is skipped, the next key starts as
// soon as any slot frees up. A slow key holds one slot and delays nothing else.
func (c *Cache) refreshSource(ctx context.Context, name string) {
	src, st := c.sources[name], c.state[name]
	list := c.trackedFor(ctx, name)
	rand.Shuffle(len(list), func(i, j int) { list[i], list[j] = list[j], list[i] })
	keys := 0
	for _, t := range list {
		if !c.warm[t.Key] {
			keys++
		}
	}
	c.mu.Lock()
	bypassed := st.bypassed
	st.keys, st.bypassed = keys, 0
	clear(st.admitted)
	c.mu.Unlock()
	if bypassed > 0 {
		c.log.Warn("source is at max_keys, new keys bypass the cache", "source", name,
			"keys", keys, "max_keys", src.MaxKeys, "bypassed", bypassed)
	}
	live := make(map[string]bool, len(list))
	for _, t := range list {
		live[t.Key] = true
		c.mu.Lock()
		busy := st.inflight[t.Key]
		c.mu.Unlock()
		if busy || ctx.Err() != nil {
			continue
		}
		select {
		case st.sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		if !c.enter() {
			<-st.sem
			return
		}
		c.mu.Lock()
		st.inflight[t.Key] = true
		c.mu.Unlock()
		st.wg.Add(1)
		go func() {
			defer c.bg.Done()
			defer st.wg.Done()
			defer func() { <-st.sem }()
			s := c.refreshKey(ctx, src, t)
			c.mu.Lock()
			delete(st.inflight, t.Key)
			if ctx.Err() == nil {
				st.stats[t.Key] = s
			}
			c.mu.Unlock()
			c.publish(name)
		}()
	}
	if ctx.Err() != nil {
		return
	}
	c.mu.Lock()
	for k := range st.stats {
		if !live[k] {
			delete(st.stats, k)
		}
	}
	c.mu.Unlock()
	c.publish(name)
	metrics.RefreshCycleLast.WithLabelValues(name).SetToCurrentTime()
}

// publish recomputes the source gauges from the last result of every tracked key.
func (c *Cache) publish(name string) {
	src, st := c.sources[name], c.state[name]
	c.mu.Lock()
	stats := make([]entryStat, 0, len(st.stats))
	for _, s := range st.stats {
		stats = append(stats, s)
	}
	inflight := len(st.inflight)
	c.mu.Unlock()

	var (
		maxAge          time.Duration
		failing         int
		gzBytes, rawLen int64
		states          = map[string]int{"fresh": 0, "stale": 0, "missing": 0}
	)
	for _, s := range stats {
		switch {
		case s.age < 0:
			states["missing"]++
			maxAge = max(maxAge, c.opt.MaxStale)
		case s.age < src.TTL:
			states["fresh"]++
		default:
			states["stale"]++
		}
		if s.age >= 0 {
			maxAge = max(maxAge, s.age)
			gzBytes += s.stored
			rawLen += s.size
		}
		if s.failing {
			failing++
		}
	}
	for state, n := range states {
		metrics.Entries.WithLabelValues(name, state).Set(float64(n))
	}
	metrics.EntriesFailing.WithLabelValues(name).Set(float64(failing))
	metrics.EntryMaxAge.WithLabelValues(name).Set(maxAge.Seconds())
	metrics.StoredBytes.WithLabelValues(name, "gzip").Set(float64(gzBytes))
	metrics.StoredBytes.WithLabelValues(name, "identity").Set(float64(rawLen))
	metrics.RefreshesInFlight.WithLabelValues(name).Set(float64(inflight))
}

func (c *Cache) refreshKey(ctx context.Context, src *source.Source, t Tracked) entryStat {
	st := entryStat{source: src.Name, request: label(t), age: -1, lastAccess: t.LastAccess}
	m := c.meta(ctx, t.Key)
	if m != nil {
		st.age = c.now().Sub(m.FetchedAt)
	}
	if m == nil || st.age+c.opt.RefreshInterval >= src.TTL {
		fctx, cancel := context.WithTimeout(ctx, c.fetchTimeout(t.Request))
		e, err := c.refresh(fctx, t.Key, t.Request, trigScheduled)
		cancel()
		switch {
		case err == nil:
			m, st.age = &e.Meta, c.now().Sub(e.Meta.FetchedAt)
		case !errors.Is(err, errLocked):
			st.failing = true
			c.logResult(t.Request, err)
		}
	}
	if st.age >= c.opt.MaxStale {
		st.age = -1
	}
	if st.age >= 0 {
		st.size, st.stored = m.Size, m.StoredSize
	}
	return st
}

// label is the request label of per-entry series; POST bodies are told apart by the key prefix.
func label(t Tracked) string {
	if len(t.Request.Body) > 0 {
		return t.Request.String() + " #" + t.Key[:8]
	}
	return t.Request.String()
}

func (c *Cache) meta(ctx context.Context, key string) *Meta {
	ctx, cancel := context.WithTimeout(ctx, storeTimeout)
	defer cancel()
	m, err := c.store.Meta(ctx, key)
	if err == nil && m != nil {
		return m
	}
	if err != nil {
		c.storeErr("meta", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if l := c.local[key]; l != nil {
		return &l.e.Meta
	}
	return nil
}

// localTracked is the refresh list when Redis is down: warmup plus what this replica holds.
func (c *Cache) localTracked(now time.Time) []Tracked {
	seen := map[string]bool{}
	var out []Tracked
	for _, r := range c.opt.Warmup {
		seen[r.Key()] = true
		out = append(out, Tracked{Key: r.Key(), Request: r, LastAccess: now})
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, l := range c.local {
		if !seen[k] && now.Sub(c.touched[k]) < c.opt.IdleTTL {
			out = append(out, Tracked{Key: k, Request: l.e.Meta.Request, LastAccess: c.touched[k]})
		}
	}
	return out
}

func (c *Cache) prune(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, l := range c.local {
		if now.Sub(l.e.Meta.FetchedAt) > c.opt.MaxStale {
			c.dropLocal(k)
		}
	}
	for k, t := range c.touched {
		if now.Sub(t) > c.opt.IdleTTL {
			delete(c.touched, k)
			c.dropLocal(k)
		}
	}
	for k, f := range c.failures {
		if now.Sub(f.At) > c.opt.ErrorBackoff {
			delete(c.failures, k)
		}
	}
}

// Describe and Collect expose replica memory and per-entry series from the last refresh pass:
// a scrape never touches Redis.
func (c *Cache) Describe(ch chan<- *prometheus.Desc) {
	ch <- metrics.LocalEntriesDesc
	ch <- metrics.LocalBytesDesc
	ch <- metrics.EntryAgeDesc
	ch <- metrics.EntrySizeDesc
	ch <- metrics.EntryAccessDesc
	ch <- metrics.EntryFailingDesc
}

func (c *Cache) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	n, size := len(c.local), c.localLen
	var snap []entryStat
	for _, st := range c.state {
		for _, s := range st.stats {
			snap = append(snap, s)
		}
	}
	c.mu.Unlock()

	ch <- prometheus.MustNewConstMetric(metrics.LocalEntriesDesc, prometheus.GaugeValue, float64(n))
	ch <- prometheus.MustNewConstMetric(metrics.LocalBytesDesc, prometheus.GaugeValue, float64(size))

	// Stalest first: the cap must keep the interesting series.
	sort.Slice(snap, func(i, j int) bool {
		ai, aj := snap[i].age, snap[j].age
		if ai < 0 || aj < 0 {
			return ai < 0 && aj >= 0
		}
		return ai > aj
	})
	emitted := 0
	for _, s := range snap {
		if emitted >= c.opt.EntryMetrics {
			break
		}
		emitted++
		if s.age >= 0 {
			ch <- prometheus.MustNewConstMetric(metrics.EntryAgeDesc, prometheus.GaugeValue, s.age.Seconds(), s.source, s.request)
			ch <- prometheus.MustNewConstMetric(metrics.EntrySizeDesc, prometheus.GaugeValue, float64(s.size), s.source, s.request)
		}
		ch <- prometheus.MustNewConstMetric(metrics.EntryAccessDesc, prometheus.GaugeValue,
			float64(s.lastAccess.Unix()), s.source, s.request)
		ch <- prometheus.MustNewConstMetric(metrics.EntryFailingDesc, prometheus.GaugeValue, b2f(s.failing), s.source, s.request)
	}
}

func (c *Cache) storeAvailable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().After(c.downTill)
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// logResult logs failures; successful fetches are logged by refresh itself, once per upstream call.
func (c *Cache) logResult(r source.Request, err error) {
	switch {
	case err == nil:
	case errors.Is(err, errBackoff), errors.Is(err, errClosed), errors.Is(err, context.Canceled):
		c.log.Debug("skipped", "request", r.String(), "error", err)
	default:
		c.log.Warn("fetch failed", "request", r.String(), "error", err)
	}
}

// storeErr logs at most every 10 s: during a Redis outage every request would log.
// Counting is done by the instrumented store.
func (c *Cache) storeErr(op string, err error) {
	c.mu.Lock()
	c.downTill = time.Now().Add(storeBreaker)
	quiet := c.now().Sub(c.lastWarn) < 10*time.Second
	if !quiet {
		c.lastWarn = c.now()
	}
	c.mu.Unlock()
	if !quiet {
		c.log.Warn("store unavailable, serving from replica memory", "op", op, "error", err)
	}
}

func newEntry(r source.Request, resp *source.Response, now time.Time) *Entry {
	return &Entry{
		Meta: Meta{
			Request:     r,
			FetchedAt:   now.Round(0),
			ContentType: resp.ContentType,
			Size:        resp.Size,
			StoredSize:  int64(len(resp.Gzip)),
			Hash:        resp.Hash,
		},
		Gzip: resp.Gzip,
	}
}

// EntryInfo is one row of the /-/entries status page.
type EntryInfo struct {
	Source     string     `json:"source"`
	Request    string     `json:"request"`
	FetchedAt  *time.Time `json:"fetched_at"`
	AgeSeconds float64    `json:"age_seconds"`
	State      string     `json:"state"` // fresh | stale | missing
	Size       int64      `json:"size"`
	StoredSize int64      `json:"stored_size"`
	LastAccess time.Time  `json:"last_access"`
	Error      string     `json:"error,omitempty"`
	ErrorAt    *time.Time `json:"error_at,omitempty"`
}

func (c *Cache) Entries(ctx context.Context) ([]EntryInfo, error) {
	now := c.now()
	tracked, err := c.store.Tracked(ctx, now.Add(-c.opt.IdleTTL))
	if err != nil {
		return nil, err
	}
	out := make([]EntryInfo, 0, len(tracked))
	for _, t := range tracked {
		if t.Key != t.Request.Key() {
			continue
		}
		info := EntryInfo{Source: t.Request.Source, Request: label(t), LastAccess: t.LastAccess, State: "missing"}
		if m := c.meta(ctx, t.Key); m != nil {
			info.FetchedAt, info.Size, info.StoredSize = new(m.FetchedAt), m.Size, m.StoredSize
			info.AgeSeconds = now.Sub(m.FetchedAt).Seconds()
			info.State = "stale"
			if src, ok := c.sources[t.Request.Source]; ok && now.Sub(m.FetchedAt) < src.TTL {
				info.State = "fresh"
			}
		}
		if f := c.failure(ctx, t.Key); f != nil {
			info.Error, info.ErrorAt = f.Msg, new(f.At)
		}
		out = append(out, info)
	}
	return out, nil
}
