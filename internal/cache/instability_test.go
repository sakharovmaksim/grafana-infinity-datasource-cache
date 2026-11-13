package cache

import (
	"context"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/metrics"
)

// A fetch that ran out of its own deadline (a hanging source, slots exhausted) is still recorded for all
// replicas, and Redis is not mistaken for being down.
func TestFetchDeadlineDoesNotBreakStore(t *testing.T) {
	up := newUpstream(t)
	up.setDelay(5 * time.Second)
	rs, _ := newRedis(t)
	c := New(rs, testSource(t, up), testOptions(), slog.New(slog.DiscardHandler))
	t.Cleanup(func() { c.Close(time.Second) })
	before := testutil.ToFloat64(metrics.StoreErrors.WithLabelValues("set_failure"))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := c.refresh(ctx, req.Key(), req, trigMiss); err == nil {
		t.Fatal("want a timeout")
	}
	if f, _ := rs.Failure(context.Background(), req.Key()); f == nil {
		t.Fatal("failure not shared with other replicas")
	}
	if d := testutil.ToFloat64(metrics.StoreErrors.WithLabelValues("set_failure")) - before; d != 0 || !c.storeAvailable() {
		t.Fatalf("store errors +%v, store available %v", d, c.storeAvailable())
	}
}

// Stopping a replica cancels its fetches; that must not put the keys into backoff for the others.
func TestShutdownIsNotAnUpstreamFailure(t *testing.T) {
	up := newUpstream(t)
	up.setDelay(5 * time.Second)
	rs, _ := newRedis(t)
	c := New(rs, testSource(t, up), testOptions(), slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	if _, err := c.refresh(ctx, req.Key(), req, trigMiss); err == nil {
		t.Fatal("want canceled")
	}
	c.Close(time.Second)
	if f, _ := rs.Failure(context.Background(), req.Key()); f != nil {
		t.Fatalf("shutdown recorded as failure: %+v", f)
	}
}

// The promise for unstable upstreams: clients keep getting the last copy during an outage, and the first
// request after recovery still answers at once while the copy is refreshed in the background.
func TestOutageAndRecovery(t *testing.T) {
	up := newUpstream(t)
	clk := &clock{t: time.Now()}
	c := newTestCache(t, NewMemoryStore(), up, clk)
	ctx := context.Background()
	if _, st, err := c.Get(ctx, req); err != nil || st != Miss {
		t.Fatalf("first: %v %v", st, err)
	}

	up.status.Store(http.StatusBadGateway)
	clk.add(2 * time.Hour)
	if e, st, err := c.Get(ctx, req); err != nil || st != Stale || body(t, e) != `{"v":1}` {
		t.Fatalf("during outage: %v %v", st, err)
	}
	eventually(t, func() bool { return up.calls.Load() == 2 }) // the background refresh failed
	if _, st, _ := c.Get(ctx, req); st != Stale || up.calls.Load() != 2 {
		t.Fatalf("in backoff: %v, upstream calls %d", st, up.calls.Load())
	}

	up.status.Store(http.StatusOK)
	up.setBody(`{"v":2}`)
	clk.add(2 * time.Minute) // past CACHE_ERROR_BACKOFF
	if e, st, _ := c.Get(ctx, req); st != Stale || body(t, e) != `{"v":1}` {
		t.Fatalf("first after recovery: %v", st)
	}
	eventually(t, func() bool { return up.calls.Load() == 3 })
	eventually(t, func() bool { e, st, _ := c.Get(ctx, req); return st == Hit && body(t, e) == `{"v":2}` })
}
