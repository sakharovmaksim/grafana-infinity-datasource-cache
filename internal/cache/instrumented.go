package cache

import (
	"context"
	"time"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/metrics"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/source"
)

// instrumented records latency and errors of every store operation.
type instrumented struct{ s Store }

func Instrument(s Store) Store {
	if _, ok := s.(instrumented); ok {
		return s
	}
	return instrumented{s}
}

func observe(op string, start time.Time, err error) {
	metrics.StoreDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	if err != nil {
		metrics.StoreErrors.WithLabelValues(op).Inc()
	}
}

func (i instrumented) Meta(ctx context.Context, key string) (*Meta, error) {
	start := time.Now()
	m, err := i.s.Meta(ctx, key)
	observe("meta", start, err)
	return m, err
}

func (i instrumented) Get(ctx context.Context, key string) (*Entry, error) {
	start := time.Now()
	e, err := i.s.Get(ctx, key)
	observe("get", start, err)
	return e, err
}

func (i instrumented) Put(ctx context.Context, key string, e *Entry, ttl time.Duration) error {
	start := time.Now()
	err := i.s.Put(ctx, key, e, ttl)
	observe("put", start, err)
	return err
}

func (i instrumented) Failure(ctx context.Context, key string) (*Failure, error) {
	start := time.Now()
	f, err := i.s.Failure(ctx, key)
	observe("failure", start, err)
	return f, err
}

func (i instrumented) SetFailure(ctx context.Context, key string, f *Failure, ttl time.Duration) error {
	start := time.Now()
	err := i.s.SetFailure(ctx, key, f, ttl)
	observe("set_failure", start, err)
	return err
}

func (i instrumented) Lock(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	start := time.Now()
	ok, err := i.s.Lock(ctx, key, owner, ttl)
	observe("lock", start, err)
	return ok, err
}

func (i instrumented) Unlock(ctx context.Context, key, owner string) error {
	start := time.Now()
	err := i.s.Unlock(ctx, key, owner)
	observe("unlock", start, err)
	return err
}

func (i instrumented) Extend(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	start := time.Now()
	ok, err := i.s.Extend(ctx, key, owner, ttl)
	observe("extend", start, err)
	return ok, err
}

func (i instrumented) Locked(ctx context.Context, key string) (bool, error) {
	start := time.Now()
	ok, err := i.s.Locked(ctx, key)
	observe("locked", start, err)
	return ok, err
}

func (i instrumented) Track(ctx context.Context, key string, r source.Request, at time.Time) error {
	start := time.Now()
	err := i.s.Track(ctx, key, r, at)
	observe("track", start, err)
	return err
}

func (i instrumented) Tracked(ctx context.Context, since time.Time) ([]Tracked, error) {
	start := time.Now()
	t, err := i.s.Tracked(ctx, since)
	observe("tracked", start, err)
	return t, err
}

func (i instrumented) Untrack(ctx context.Context, before time.Time) error {
	start := time.Now()
	err := i.s.Untrack(ctx, before)
	observe("untrack", start, err)
	return err
}

func (i instrumented) Forget(ctx context.Context, key string) error {
	start := time.Now()
	err := i.s.Forget(ctx, key)
	observe("forget", start, err)
	return err
}

func (i instrumented) Ping(ctx context.Context) error {
	start := time.Now()
	err := i.s.Ping(ctx)
	observe("ping", start, err)
	return err
}
