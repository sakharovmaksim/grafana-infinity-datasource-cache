package cache

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/expfmt"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/metrics"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/source"
)

func delta(t *testing.T, get func() float64, want float64, action func()) {
	t.Helper()
	before := get()
	action()
	if got := get() - before; got != want {
		t.Fatalf("delta = %v, want %v", got, want)
	}
}

func refreshes(trigger, result string) func() float64 {
	return func() float64 { return testutil.ToFloat64(metrics.Refreshes.WithLabelValues("up", trigger, result)) }
}

func TestRefreshTriggerMetrics(t *testing.T) {
	up := newUpstream(t)
	clk := &clock{t: time.Now()}
	c := newTestCache(t, NewMemoryStore(), up, clk)
	ctx := context.Background()

	delta(t, refreshes(trigMiss, "success"), 1, func() { _, _, _ = c.Get(ctx, req) })

	clk.add(2 * time.Hour)
	before := refreshes(trigStale, "success")()
	_, _, _ = c.Get(ctx, req)
	eventually(t, func() bool { return refreshes(trigStale, "success")()-before == 1 })

	up.status.Store(http.StatusBadGateway)
	clk.add(2 * time.Hour)
	delta(t, refreshes(trigScheduled, "error"), 1, func() { c.tick(ctx) })
	delta(t, refreshes(trigScheduled, "backoff"), 1, func() { c.tick(ctx) })
	if v := testutil.ToFloat64(metrics.EntriesFailing.WithLabelValues("up")); v != 1 {
		t.Fatalf("entries_failing = %v", v)
	}
	if v := testutil.ToFloat64(metrics.Entries.WithLabelValues("up", "stale")); v != 1 {
		t.Fatalf("entries{stale} = %v", v)
	}
}

func TestLookupMetrics(t *testing.T) {
	up := newUpstream(t)
	clk := &clock{t: time.Now()}
	c := newTestCache(t, NewMemoryStore(), up, clk)
	ctx := context.Background()
	lookups := func(result string) func() float64 {
		return func() float64 { return testutil.ToFloat64(metrics.Lookups.WithLabelValues(result)) }
	}
	delta(t, lookups("absent"), 1, func() { _, _, _ = c.Get(ctx, req) })
	delta(t, lookups("local"), 1, func() { _, _, _ = c.Get(ctx, req) })
	// Past CACHE_LOCAL_CHECK the answer is still local; the store check runs in the background.
	current := func() float64 { return testutil.ToFloat64(metrics.Revalidations.WithLabelValues("current")) }
	before := current()
	clk.add(time.Minute)
	delta(t, lookups("local"), 1, func() { _, _, _ = c.Get(ctx, req) })
	eventually(t, func() bool { return current()-before == 1 })
}

// A second request in the same replica joins the running fetch; one in another replica waits on the lock.
func TestCoalescingAndLockWaitMetrics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := NewMemoryStore() // shared by both replicas, as Redis would be
		up := newUpstream(t)
		up.setDelay(300 * time.Millisecond)
		clk := &clock{t: time.Now()}
		a := newTestCache(t, store, up, clk)
		b := newTestCache(t, store, up, clk)
		a.opt.Owner, b.opt.Owner = "a", "b"

		coalesced := testutil.ToFloat64(metrics.Coalesced.WithLabelValues("up"))
		waited := testutil.ToFloat64(metrics.LockWaits.WithLabelValues("up", "entry"))
		var wg sync.WaitGroup
		wg.Go(func() { _, _, _ = a.Get(context.Background(), req) })
		synctest.Wait() // the first fetch is now waiting on the upstream
		wg.Go(func() { _, _, _ = a.Get(context.Background(), req) })
		wg.Go(func() { _, _, _ = b.Get(context.Background(), req) })
		wg.Wait()

		if d := testutil.ToFloat64(metrics.Coalesced.WithLabelValues("up")) - coalesced; d != 1 {
			t.Errorf("coalesced delta = %v", d)
		}
		if d := testutil.ToFloat64(metrics.LockWaits.WithLabelValues("up", "entry")) - waited; d != 1 {
			t.Errorf("lock_waits{entry} delta = %v", d)
		}
		if n := up.calls.Load(); n != 1 {
			t.Errorf("upstream calls = %d", n)
		}
	})
}

func TestEntryCollector(t *testing.T) {
	up := newUpstream(t)
	clk := &clock{t: time.Now()}
	c := newTestCache(t, NewMemoryStore(), up, clk)
	c.opt.EntryMetrics = 1
	post := source.NewRequest("up", "POST", "/q", "", "application/json", []byte(`{"a":1}`))
	c.opt.Warmup = []source.Request{req}
	if _, _, err := c.Get(context.Background(), post); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { l, _ := c.store.Tracked(context.Background(), time.Time{}); return len(l) == 1 })
	clk.add(10 * time.Minute) // the POST entry becomes the older one
	c.tick(context.Background())

	if n := testutil.CollectAndCount(c, "datasource_cache_entry_age_seconds"); n != 1 {
		t.Fatalf("entry_age series = %d, want 1 (capped)", n)
	}
	out, _ := testutil.CollectAndFormat(c, expfmt.TypeTextPlain, "datasource_cache_entry_age_seconds")
	if !strings.Contains(string(out), `request="POST /up/q #`) {
		t.Fatalf("stalest entry must survive the cap:\n%s", out)
	}
	if v := testutil.ToFloat64(metrics.StoredBytes.WithLabelValues("up", "identity")); v != 14 {
		t.Fatalf("stored identity bytes = %v", v)
	}
	if problems, err := testutil.CollectAndLint(c); err != nil || len(problems) > 0 {
		t.Fatalf("lint: %v %v", problems, err)
	}

	c.opt.EntryMetrics = 0
	if n := testutil.CollectAndCount(c, "datasource_cache_entry_age_seconds"); n != 0 {
		t.Fatalf("METRICS_ENTRY_LIMIT=0 must disable per-entry series, got %d", n)
	}
}

func TestRedisMetrics(t *testing.T) {
	rs, mr := newRedis(t)
	if n := testutil.CollectAndCount(rs.Collector()); n != 8 {
		t.Fatalf("pool series = %d", n)
	}
	if problems, err := testutil.CollectAndLint(rs.Collector()); err != nil || len(problems) > 0 {
		t.Fatalf("lint: %v %v", problems, err)
	}
	s := Instrument(rs)
	errs := func() float64 { return testutil.ToFloat64(metrics.StoreErrors.WithLabelValues("meta")) }
	mr.Close()
	delta(t, errs, 1, func() { _, _ = s.Meta(context.Background(), "k") })

	up := newUpstream(t)
	c := newTestCache(t, rs, up, &clock{t: time.Now()})
	c.tick(context.Background())
	if v := testutil.ToFloat64(metrics.StoreUp); v != 0 {
		t.Fatalf("store_up = %v with Redis down", v)
	}
}
