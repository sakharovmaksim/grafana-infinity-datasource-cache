package cache

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/metrics"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/source"
)

func path(p string) source.Request { return source.NewRequest("up", "GET", p, "", "", nil) }

// A URL with a time range is a new key on every dashboard refresh: above max_keys new keys are served
// but cost neither memory, nor Redis, nor background refreshes.
func TestMaxKeysBypassesNewKeys(t *testing.T) {
	up := newUpstream(t)
	clk := &clock{t: time.Now()}
	store := NewMemoryStore()
	store.now = clk.now
	srcs := testSource(t, up)
	srcs["up"].MaxKeys = 2
	opt := testOptions()
	opt.Warmup = []source.Request{path("/warm")}
	c := New(store, srcs, opt, slog.New(slog.DiscardHandler))
	t.Cleanup(func() { c.Close(time.Minute) })
	c.now = clk.now
	ctx := context.Background()

	c.tick(ctx) // warmup is not counted against max_keys
	for _, p := range []string{"/a", "/b"} {
		if _, st, err := c.Get(ctx, path(p)); err != nil || st != Miss {
			t.Fatalf("%s: %v %v", p, st, err)
		}
	}
	calls := up.calls.Load()
	for range 2 {
		e, st, err := c.Get(ctx, path("/c"))
		if err != nil || st != Bypass || body(t, e) != `{"v":1}` {
			t.Fatalf("/c: %v %v", st, err)
		}
	}
	if n := up.calls.Load() - calls; n != 2 {
		t.Fatalf("bypassed requests went upstream %d times, want 2", n)
	}
	if e, _ := store.Get(ctx, path("/c").Key()); e != nil {
		t.Fatal("bypassed response was stored")
	}
	if _, st, _ := c.Get(ctx, path("/a")); st != Hit {
		t.Fatalf("admitted key: %v", st)
	}

	// The pass recounts from the store: still full, and the bypassed key is not tracked.
	eventually(t, func() bool { l, _ := store.Tracked(ctx, time.Time{}); return len(l) == 3 })
	c.tick(ctx)
	if _, st, _ := c.Get(ctx, path("/d")); st != Bypass {
		t.Fatalf("after pass: %v", st)
	}
	// Idle keys free their slots.
	clk.add(73 * time.Hour)
	c.tick(ctx)
	if _, st, _ := c.Get(ctx, path("/d")); st != Miss {
		t.Fatalf("after idle: %v", st)
	}
}

func TestLocalCopiesStayWithinLimit(t *testing.T) {
	up := newUpstream(t)
	clk := &clock{t: time.Now()}
	store := NewMemoryStore()
	c := newTestCache(t, store, up, clk)
	ctx := context.Background()

	e, _, err := c.Get(ctx, path("/a"))
	if err != nil {
		t.Fatal(err)
	}
	size := int64(len(e.Gzip))
	c.opt.LocalMaxBytes = 2*size + 1
	clk.add(time.Second)
	_, _, _ = c.Get(ctx, path("/b"))
	clk.add(time.Second)
	_, _, _ = c.Get(ctx, path("/a")) // /b is now the least recently read
	clk.add(time.Second)

	before := testutil.ToFloat64(metrics.LocalEvictions)
	_, _, _ = c.Get(ctx, path("/c"))
	c.mu.Lock()
	_, hasA := c.local[path("/a").Key()]
	_, hasB := c.local[path("/b").Key()]
	n, held := len(c.local), c.localLen
	c.mu.Unlock()
	if !hasA || hasB || n != 2 || held != 2*size {
		t.Fatalf("local: a=%v b=%v n=%d bytes=%d", hasA, hasB, n, held)
	}
	if d := testutil.ToFloat64(metrics.LocalEvictions) - before; d != 1 {
		t.Fatalf("evictions += %v", d)
	}
	// The evicted copy comes back from the store, not from the upstream.
	calls := up.calls.Load()
	if _, st, _ := c.Get(ctx, path("/b")); st != Hit || up.calls.Load() != calls {
		t.Fatalf("evicted key: %v, upstream calls +%d", st, up.calls.Load()-calls)
	}
}

func TestStoredRequestsRespectNarrowedPolicy(t *testing.T) {
	up := newUpstream(t)
	store := NewMemoryStore()
	srcs := testSource(t, up)
	srcs["up"].Paths = []string{"/allowed"}
	c := New(store, srcs, testOptions(), slog.New(slog.DiscardHandler))
	defer c.Close(time.Second)
	ctx := context.Background()
	for _, p := range []string{"/allowed", "/revoked"} {
		r := source.NewRequest("up", "GET", p, "", "", nil)
		if err := store.Track(ctx, r.Key(), r, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	c.tick(ctx)
	if up.pathCalls("/revoked") != 0 {
		t.Fatal("background refresh called revoked path")
	}
	if up.pathCalls("/allowed") != 1 {
		t.Fatal("allowed request was not refreshed")
	}
}

func TestLegacyKeysAreNotServedOrRefreshed(t *testing.T) {
	rs, _ := newRedis(t)
	for name, store := range map[string]Store{"memory": NewMemoryStore(), "redis": rs} {
		t.Run(name, func(t *testing.T) { testLegacyKeys(t, store) })
	}
}

func testLegacyKeys(t *testing.T, store Store) {
	t.Helper()
	up := newUpstream(t)
	r := source.NewRequest("up", "GET", "/x", "", "", nil)
	// Fixed v1 key for "up\nGET\n/x\n\n"; v1 records cannot be safely migrated.
	const legacyKey = "6ec8441a817594202767b49b26274e32"
	ctx := context.Background()
	legacy := &Entry{Meta: Meta{Request: r, FetchedAt: time.Now().Add(-2 * time.Hour)}, Gzip: []byte("legacy")}
	if err := store.Put(ctx, legacyKey, legacy, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := store.Track(ctx, legacyKey, r, time.Now()); err != nil {
		t.Fatal(err)
	}
	opt := testOptions()
	opt.Warmup = []source.Request{r}
	c := New(store, testSource(t, up), opt, slog.New(slog.DiscardHandler))
	defer c.Close(time.Second)
	c.tick(ctx)
	if n := up.calls.Load(); n != 1 {
		t.Fatalf("warmup calls = %d, want 1 (legacy must not refresh)", n)
	}
	e, status, err := c.Get(ctx, r)
	if err != nil || status != Hit || e == nil || string(e.Gzip) == "legacy" {
		t.Fatalf("warmup was not used: status=%s err=%v", status, err)
	}
	entries, err := c.Entries(ctx)
	if err != nil || len(entries) != 1 {
		t.Fatalf("entries=%v err=%v, want only v2 record", entries, err)
	}
	old, err := store.Get(ctx, legacyKey)
	if err != nil || old == nil || string(old.Gzip) != "legacy" || !old.Meta.FetchedAt.Equal(legacy.Meta.FetchedAt) {
		t.Fatal("legacy entry changed while old replicas may still use it")
	}
	if r.Key() == legacyKey {
		t.Fatal("new request reused a legacy key")
	}
}
