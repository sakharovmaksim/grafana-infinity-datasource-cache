package cache

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/source"
)

type upstream struct {
	*httptest.Server
	calls  atomic.Int32
	status atomic.Int32
	delay  atomic.Int64
	mu     sync.Mutex
	body   string
	paths  map[string]int
}

func (u *upstream) setDelay(d time.Duration) { u.delay.Store(int64(d)) }

func (u *upstream) pathCalls(p string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.paths[p]
}

func newUpstream(t *testing.T) *upstream {
	u := &upstream{body: `{"v":1}`, paths: map[string]int{}}
	u.status.Store(http.StatusOK)
	u.Server = httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.calls.Add(1)
		u.mu.Lock()
		u.paths[r.URL.Path]++
		u.mu.Unlock()
		delay := time.Duration(u.delay.Load())
		if r.URL.Path == "/slow" {
			delay = 1500 * time.Millisecond
		}
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
		u.mu.Lock()
		body := u.body
		u.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(int(u.status.Load()))
		_, _ = io.WriteString(w, body)
	}))
	u.Client() // sets u.URL; requests must go through this client (in-memory network)
	return u
}

func (u *upstream) setBody(b string) {
	u.mu.Lock()
	u.body = b
	u.mu.Unlock()
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) add(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func testSource(t *testing.T, up *upstream) map[string]*source.Source {
	t.Helper()
	u, err := url.Parse(up.URL)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]*source.Source{"up": {
		Name: "up", URL: u, Paths: []string{"/"}, Methods: []string{"GET", "POST"},
		TTL: time.Hour, Timeout: 2 * time.Second, Attempts: 1, ValidateJSON: true,
		MaxBodyBytes: 1 << 20, Client: up.Client(),
	}}
}

func testOptions() Options {
	return Options{
		MaxStale: 24 * time.Hour, IdleTTL: 72 * time.Hour, RefreshInterval: time.Minute,
		ErrorBackoff: time.Minute, MissWait: 5 * time.Second, LocalCheck: 10 * time.Second, Owner: "test",
	}
}

func newTestCache(t *testing.T, store Store, up *upstream, clk *clock) *Cache {
	c := New(store, testSource(t, up), testOptions(), slog.New(slog.DiscardHandler))
	t.Cleanup(func() { c.Close(time.Minute) })
	c.now = clk.now
	if m, ok := store.(*MemoryStore); ok {
		m.now = clk.now
	}
	return c
}

func body(t *testing.T, e *Entry) string {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(e.Gzip))
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

var req = source.NewRequest("up", "GET", "/cves", "product=go&noRejected", "", nil)

func TestMissThenHit(t *testing.T) {
	up := newUpstream(t)
	c := newTestCache(t, NewMemoryStore(), up, &clock{t: time.Now()})

	e, st, err := c.Get(context.Background(), req)
	if err != nil || st != Miss || body(t, e) != `{"v":1}` {
		t.Fatalf("first: %v %v", st, err)
	}
	if _, st, _ = c.Get(context.Background(), req); st != Hit {
		t.Fatalf("second: %v", st)
	}
	if up.calls.Load() != 1 {
		t.Fatalf("upstream calls = %d", up.calls.Load())
	}
}

func TestStaleServedAndRefreshedInBackground(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		up := newUpstream(t)
		clk := &clock{t: time.Now()}
		c := newTestCache(t, NewMemoryStore(), up, clk)
		if _, _, err := c.Get(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		up.setBody(`{"v":2}`)
		clk.add(2 * time.Hour)

		e, st, err := c.Get(context.Background(), req)
		if err != nil || st != Stale || body(t, e) != `{"v":1}` {
			t.Fatalf("stale: %v %v", st, err)
		}
		eventually(t, func() bool {
			e, st, _ := c.Get(context.Background(), req)
			return st == Hit && body(t, e) == `{"v":2}`
		})
	})
}

func TestStaleKeptWhileUpstreamFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		up := newUpstream(t)
		clk := &clock{t: time.Now()}
		c := newTestCache(t, NewMemoryStore(), up, clk)
		if _, _, err := c.Get(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		up.status.Store(http.StatusServiceUnavailable)
		clk.add(2 * time.Hour)

		for range 5 {
			e, st, err := c.Get(context.Background(), req)
			if err != nil || st != Stale || body(t, e) != `{"v":1}` {
				t.Fatalf("stale: %v %v", st, err)
			}
			synctest.Sleep(20 * time.Millisecond)
		}
		// One failed refresh, then backoff: the failing upstream is not hammered.
		if n := up.calls.Load(); n != 2 {
			t.Fatalf("upstream calls = %d, want 2", n)
		}
	})
}

func TestMaxStaleExceededFailsExplicitly(t *testing.T) {
	up := newUpstream(t)
	clk := &clock{t: time.Now()}
	c := newTestCache(t, NewMemoryStore(), up, clk)
	if _, _, err := c.Get(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	up.status.Store(http.StatusBadGateway)
	clk.add(25 * time.Hour)
	if _, _, err := c.Get(context.Background(), req); err == nil {
		t.Fatal("want error past CACHE_MAX_STALE")
	}
}

func TestColdMissFailureBacksOff(t *testing.T) {
	up := newUpstream(t)
	up.status.Store(http.StatusInternalServerError)
	c := newTestCache(t, NewMemoryStore(), up, &clock{t: time.Now()})
	for range 3 {
		if _, _, err := c.Get(context.Background(), req); err == nil {
			t.Fatal("want error")
		}
	}
	if n := up.calls.Load(); n != 1 {
		t.Fatalf("upstream calls = %d, want 1", n)
	}
}

func TestClientErrorPassedThrough(t *testing.T) {
	up := newUpstream(t)
	up.status.Store(http.StatusNotFound)
	c := newTestCache(t, NewMemoryStore(), up, &clock{t: time.Now()})
	_, _, err := c.Get(context.Background(), req)
	if code, ok := source.ClientError(err); !ok || code != http.StatusNotFound {
		t.Fatalf("err = %v", err)
	}
}

func TestInvalidJSONNotCached(t *testing.T) {
	up := newUpstream(t)
	up.setBody(`<html>maintenance</html>`)
	c := newTestCache(t, NewMemoryStore(), up, &clock{t: time.Now()})
	if _, _, err := c.Get(context.Background(), req); err == nil {
		t.Fatal("want error")
	}
}

func TestPostBodyIsPartOfKey(t *testing.T) {
	a := source.NewRequest("up", "POST", "/v1/query", "", "application/json", []byte(`{"a":1}`))
	b := source.NewRequest("up", "POST", "/v1/query", "", "application/json", []byte(`{"b":1}`))
	if a.Key() == b.Key() {
		t.Fatal("different bodies share a key")
	}
	up := newUpstream(t)
	c := newTestCache(t, NewMemoryStore(), up, &clock{t: time.Now()})
	for _, r := range []source.Request{a, b, a} {
		if _, _, err := c.Get(context.Background(), r); err != nil {
			t.Fatal(err)
		}
	}
	if n := up.calls.Load(); n != 2 {
		t.Fatalf("upstream calls = %d, want 2", n)
	}
}

func TestTickRefreshesAheadAndWarmsUp(t *testing.T) {
	up := newUpstream(t)
	clk := &clock{t: time.Now()}
	store := NewMemoryStore()
	c := newTestCache(t, store, up, clk)
	c.opt.Warmup = []source.Request{req}

	c.tick(context.Background())
	if n := up.calls.Load(); n != 1 {
		t.Fatalf("warmup calls = %d", n)
	}
	clk.add(30 * time.Minute)
	c.tick(context.Background())
	if n := up.calls.Load(); n != 1 {
		t.Fatalf("fresh entry refreshed: calls = %d", n)
	}
	// Within one refresh interval of the TTL: refreshed before clients see it stale.
	clk.add(29*time.Minute + 30*time.Second)
	c.tick(context.Background())
	if n := up.calls.Load(); n != 2 {
		t.Fatalf("calls = %d, want 2", n)
	}
	if _, st, _ := c.Get(context.Background(), req); st != Hit {
		t.Fatalf("status = %v", st)
	}
}

func TestIdleKeysStopRefreshing(t *testing.T) {
	up := newUpstream(t)
	clk := &clock{t: time.Now()}
	store := NewMemoryStore()
	c := newTestCache(t, store, up, clk)
	if _, _, err := c.Get(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { l, _ := store.Tracked(context.Background(), time.Time{}); return len(l) == 1 })
	clk.add(73 * time.Hour)
	c.tick(context.Background())
	if n := up.calls.Load(); n != 1 {
		t.Fatalf("idle key refreshed: calls = %d", n)
	}
}

func TestMissWaitTimeoutKeepsFetching(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		up := newUpstream(t)
		up.setDelay(300 * time.Millisecond)
		c := newTestCache(t, NewMemoryStore(), up, &clock{t: time.Now()})
		c.opt.MissWait = 50 * time.Millisecond
		if _, _, err := c.Get(context.Background(), req); !errors.Is(err, ErrTimeout) {
			t.Fatalf("err = %v", err)
		}
		eventually(t, func() bool { _, st, _ := c.Get(context.Background(), req); return st == Hit })
	})
}

// A slow source (NVD feeds take minutes) may wait longer than the default, or shorter.
func TestSourceMissWaitOverridesDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		up := newUpstream(t)
		up.setDelay(300 * time.Millisecond)
		c := newTestCache(t, NewMemoryStore(), up, &clock{t: time.Now()})
		c.opt.MissWait = 50 * time.Millisecond
		c.sources["up"].MissWait = time.Second
		if _, st, err := c.Get(context.Background(), req); err != nil || st != Miss {
			t.Fatalf("source miss_wait 1s, upstream 300ms: %v %v", st, err)
		}
	})
}

func TestInvalidQueryIsNotTracked(t *testing.T) {
	up := newUpstream(t)
	up.status.Store(http.StatusNotFound)
	clk := &clock{t: time.Now()}
	store := NewMemoryStore()
	c := newTestCache(t, store, up, clk)
	if _, _, err := c.Get(context.Background(), req); err == nil {
		t.Fatal("want 404")
	}
	c.tick(context.Background()) // the async track may land after the forget; the pass cleans it up
	if l, _ := store.Tracked(context.Background(), time.Time{}); len(l) != 0 {
		t.Fatalf("invalid query still tracked: %+v", l)
	}

	// A key with data that starts failing with 4xx stays tracked: it is a real problem.
	up.status.Store(http.StatusOK)
	good := source.NewRequest("up", "GET", "/good", "", "", nil)
	if _, _, err := c.Get(context.Background(), good); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { l, _ := store.Tracked(context.Background(), time.Time{}); return len(l) == 1 })
	up.status.Store(http.StatusForbidden)
	clk.add(2 * time.Hour)
	c.tick(context.Background())
	if l, _ := store.Tracked(context.Background(), time.Time{}); len(l) != 1 {
		t.Fatalf("key with data must stay tracked: %+v", l)
	}
}
