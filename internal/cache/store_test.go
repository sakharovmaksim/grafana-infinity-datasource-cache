package cache

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newRedis(t *testing.T) (*RedisStore, *miniredis.Miniredis) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewRedisStore(rdb, "test:"), mr
}

func TestStores(t *testing.T) {
	rs, _ := newRedis(t)
	for name, s := range map[string]Store{"memory": NewMemoryStore(), "redis": rs} {
		t.Run(name, func(t *testing.T) { testStore(t, s) })
	}
}

func testStore(t *testing.T, s Store) {
	ctx := context.Background()
	now := time.Now().Round(0)

	if m, err := s.Meta(ctx, "k"); m != nil || err != nil {
		t.Fatalf("absent meta: %v %v", m, err)
	}
	e := &Entry{Meta: Meta{Request: req, FetchedAt: now, Hash: "h", Size: 3}, Gzip: []byte{1, 2, 3}}
	if err := s.Put(ctx, "k", e, time.Hour); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "k")
	if err != nil || got == nil || got.Meta.Hash != "h" || !got.Meta.FetchedAt.Equal(now) || string(got.Gzip) != "\x01\x02\x03" {
		t.Fatalf("get: %+v %v", got, err)
	}
	if got.Meta.Request.Query != "product=go&noRejected" {
		t.Fatalf("request not round-tripped: %+v", got.Meta.Request)
	}

	if err := s.SetFailure(ctx, "k", &Failure{Msg: "boom", Status: 503, Reason: "timeout", At: now}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if f, _ := s.Failure(ctx, "k"); f == nil || f.Status != 503 || f.Reason != "timeout" {
		t.Fatalf("failure: %+v", f)
	}
	if err := s.Put(ctx, "k", e, time.Hour); err != nil {
		t.Fatal(err)
	}
	if f, _ := s.Failure(ctx, "k"); f != nil {
		t.Fatal("Put must clear the failure")
	}

	if ok, _ := s.Lock(ctx, "k", "a", time.Minute); !ok {
		t.Fatal("lock a")
	}
	if ok, _ := s.Lock(ctx, "k", "b", time.Minute); ok {
		t.Fatal("lock b while a holds it")
	}
	_ = s.Unlock(ctx, "k", "b")
	if locked, _ := s.Locked(ctx, "k"); !locked {
		t.Fatal("foreign unlock released the lock")
	}
	_ = s.Unlock(ctx, "k", "a")
	if locked, _ := s.Locked(ctx, "k"); locked {
		t.Fatal("still locked")
	}

	_ = s.Track(ctx, "old", req, now.Add(-2*time.Hour))
	_ = s.Track(ctx, "new", req, now)
	_ = s.Put(ctx, "old", e, time.Hour)
	_ = s.Put(ctx, "new", e, time.Hour)
	if err := s.Untrack(ctx, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if old, _ := s.Get(ctx, "old"); old != nil {
		t.Fatal("the entry of an idle key must go with it")
	}
	if kept, _ := s.Get(ctx, "new"); kept == nil {
		t.Fatal("entry of a tracked key removed")
	}
	list, err := s.Tracked(ctx, time.Time{})
	if err != nil || len(list) != 1 || list[0].Key != "new" || list[0].Request.Path != "/cves" {
		t.Fatalf("tracked: %+v %v", list, err)
	}
}

// Two replicas sharing Redis fetch a cold key from the upstream once.
func TestReplicasShareOneFetch(t *testing.T) {
	rs, _ := newRedis(t)
	up := newUpstream(t)
	up.setDelay(200 * time.Millisecond)
	clk := &clock{t: time.Now()}
	a := newTestCache(t, rs, up, clk)
	b := newTestCache(t, rs, up, clk)
	a.opt.Owner, b.opt.Owner = "a", "b"

	var wg sync.WaitGroup
	for _, c := range []*Cache{a, b, a, b} {
		wg.Go(func() {
			e, _, err := c.Get(context.Background(), req)
			if err != nil || body(t, e) != `{"v":1}` {
				t.Errorf("get: %v", err)
			}
		})
	}
	wg.Wait()
	if n := up.calls.Load(); n != 1 {
		t.Fatalf("upstream calls = %d, want 1", n)
	}
}

// With Redis gone the replica keeps serving its own copy.
func TestRedisOutageServesLocalCopy(t *testing.T) {
	rs, mr := newRedis(t)
	up := newUpstream(t)
	clk := &clock{t: time.Now()}
	c := newTestCache(t, rs, up, clk)
	c.log = slog.New(slog.DiscardHandler)
	if _, _, err := c.Get(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	mr.Close()
	clk.add(time.Minute) // past CACHE_LOCAL_CHECK: the replica asks Redis and fails
	e, st, err := c.Get(context.Background(), req)
	if err != nil || st != Hit || body(t, e) != `{"v":1}` {
		t.Fatalf("outage: %v %v", st, err)
	}
	// The breaker is open now: the next request does not wait for Redis dial retries.
	clk.add(time.Minute)
	start := time.Now()
	if _, st, _ := c.Get(context.Background(), req); st != Hit || time.Since(start) > 100*time.Millisecond {
		t.Fatalf("breaker: %v in %v", st, time.Since(start))
	}
}
