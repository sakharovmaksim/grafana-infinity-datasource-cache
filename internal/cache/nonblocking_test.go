package cache

import (
	"context"
	"log/slog"
	"net/url"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/metrics"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/source"
)

// Tests here run in synctest bubbles: time is virtual and advances only when every goroutine waits,
// so "took under 100 ms" means "did not wait on any timer", and the outcome does not depend on load.

// slowStore delays reads, as a struggling Redis would.
type slowStore struct {
	*MemoryStore
	delay atomic.Int64
}

func (s *slowStore) wait() { time.Sleep(time.Duration(s.delay.Load())) }

func (s *slowStore) Meta(ctx context.Context, key string) (*Meta, error) {
	s.wait()
	return s.MemoryStore.Meta(ctx, key)
}

func (s *slowStore) Get(ctx context.Context, key string) (*Entry, error) {
	s.wait()
	return s.MemoryStore.Get(ctx, key)
}

func fast(t *testing.T, limit time.Duration, fn func()) {
	t.Helper()
	start := time.Now()
	fn()
	if d := time.Since(start); d > limit {
		t.Fatalf("took %v, want under %v", d, limit)
	}
}

func TestHitDoesNotWaitForStore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		up := newUpstream(t)
		clk := &clock{t: time.Now()}
		store := &slowStore{MemoryStore: NewMemoryStore()}
		store.now = clk.now
		c := newTestCache(t, store, up, clk)
		if _, _, err := c.Get(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		store.delay.Store(int64(time.Second))
		clk.add(time.Minute) // past CACHE_LOCAL_CHECK: a store check is due
		fast(t, 100*time.Millisecond, func() {
			if _, st, _ := c.Get(context.Background(), req); st != Hit {
				t.Fatalf("status %v", st)
			}
		})
	})
}

func TestStaleDoesNotWaitForUpstream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		up := newUpstream(t)
		clk := &clock{t: time.Now()}
		c := newTestCache(t, NewMemoryStore(), up, clk)
		if _, _, err := c.Get(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		up.setDelay(2 * time.Second)
		clk.add(2 * time.Hour)
		fast(t, 100*time.Millisecond, func() {
			if _, st, _ := c.Get(context.Background(), req); st != Stale {
				t.Fatalf("status %v", st)
			}
		})
		c.Close(time.Second)
	})
}

// A slow key delays neither other keys of its source nor other sources; Close aborts it and frees the lock.
func TestSlowKeyDoesNotBlockRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		up := newUpstream(t)
		u, _ := url.Parse(up.URL)
		srcs := map[string]*source.Source{}
		for _, name := range []string{"up", "other"} {
			srcs[name] = &source.Source{
				Name: name, URL: u, Paths: []string{"/"}, Methods: []string{"GET"},
				TTL: 50 * time.Millisecond, Timeout: 5 * time.Second, Attempts: 1, Concurrency: 4,
				ValidateJSON: true, MaxBodyBytes: 1 << 20, Client: up.Client(),
			}
		}
		store := NewMemoryStore()
		opt := testOptions()
		opt.RefreshInterval = 100 * time.Millisecond
		slow := source.NewRequest("up", "GET", "/slow", "", "", nil)
		opt.Warmup = []source.Request{
			slow,
			source.NewRequest("up", "GET", "/fast", "", "", nil),
			source.NewRequest("other", "GET", "/fast2", "", "", nil),
		}
		c := New(store, srcs, opt, slog.New(slog.DiscardHandler))

		ctx, cancel := context.WithCancel(context.Background())
		go c.Run(ctx)
		synctest.Sleep(time.Second)
		if n := up.pathCalls("/slow"); n != 1 {
			t.Fatalf("/slow calls = %d, want 1 (still in its first fetch)", n)
		}
		for _, p := range []string{"/fast", "/fast2"} {
			if n := up.pathCalls(p); n < 4 {
				t.Errorf("%s refreshed %d times while /slow hung, want ≥ 4", p, n)
			}
		}

		cancel()
		fast(t, time.Second, func() { c.Close(5 * time.Second) })
		if locked, _ := store.Locked(context.Background(), slow.Key()); locked {
			t.Fatal("Close left the fetch lock behind")
		}
	})
}

// A replica that died mid-fetch blocks the key for one lease, not for the whole upstream timeout.
func TestDeadReplicaLockExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		up := newUpstream(t)
		store := NewMemoryStore()
		c := New(store, testSource(t, up), testOptions(), slog.New(slog.DiscardHandler))
		c.opt.LockLease = 300 * time.Millisecond
		if ok, _ := store.Lock(context.Background(), req.Key(), "dead", c.opt.LockLease); !ok {
			t.Fatal("lock")
		}
		fast(t, 2*time.Second, func() {
			if _, _, err := c.Get(context.Background(), req); err != nil {
				t.Fatal(err)
			}
		})
		if n := up.calls.Load(); n != 1 {
			t.Fatalf("upstream calls = %d", n)
		}
	})
}

func TestHeartbeatKeepsLockDuringLongFetch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		up := newUpstream(t)
		up.setDelay(700 * time.Millisecond)
		store := NewMemoryStore()
		c := New(store, testSource(t, up), testOptions(), slog.New(slog.DiscardHandler))
		c.opt.LockLease = 150 * time.Millisecond
		done := make(chan struct{})
		go func() { _, _, _ = c.Get(context.Background(), req); close(done) }()
		synctest.Sleep(450 * time.Millisecond) // three leases
		if locked, _ := store.Locked(context.Background(), req.Key()); !locked {
			t.Fatal("lease expired mid-fetch: another replica would fetch the same key")
		}
		<-done
		if locked, _ := store.Locked(context.Background(), req.Key()); locked {
			t.Fatal("lock not released")
		}
	})
}

// A replica that lost the race to another replica's refresh reuses its result.
func TestRefreshReusesFreshStoreCopy(t *testing.T) {
	up := newUpstream(t)
	clk := &clock{t: time.Now()}
	store := NewMemoryStore()
	a := newTestCache(t, store, up, clk)
	b := newTestCache(t, store, up, clk)
	if _, _, err := a.Get(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	current := func() float64 {
		return testutil.ToFloat64(metrics.Refreshes.WithLabelValues("up", trigStale, "current"))
	}
	delta(t, current, 1, func() {
		if _, err := b.refresh(context.Background(), req.Key(), req, trigStale); err != nil {
			t.Fatal(err)
		}
	})
	if n := up.calls.Load(); n != 1 {
		t.Fatalf("upstream calls = %d, want 1", n)
	}
}
