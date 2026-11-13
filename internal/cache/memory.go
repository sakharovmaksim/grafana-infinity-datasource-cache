package cache

import (
	"context"
	"sync"
	"time"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/source"
)

// MemoryStore keeps everything in process memory: for local runs and tests.
type MemoryStore struct {
	mu       sync.Mutex
	now      func() time.Time
	entries  map[string]expiring[*Entry]
	failures map[string]expiring[*Failure]
	locks    map[string]expiring[string]
	tracked  map[string]Tracked
}

type expiring[T any] struct {
	v   T
	exp time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		now:      time.Now,
		entries:  map[string]expiring[*Entry]{},
		failures: map[string]expiring[*Failure]{},
		locks:    map[string]expiring[string]{},
		tracked:  map[string]Tracked{},
	}
}

func get[T any](m map[string]expiring[T], key string, now time.Time) (T, bool) {
	e, ok := m[key]
	if !ok || !now.Before(e.exp) {
		delete(m, key)
		var zero T
		return zero, false
	}
	return e.v, true
}

func (s *MemoryStore) Meta(_ context.Context, key string) (*Meta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := get(s.entries, key, s.now()); ok {
		m := e.Meta
		return &m, nil
	}
	return nil, nil
}

func (s *MemoryStore) Get(_ context.Context, key string) (*Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := get(s.entries, key, s.now()); ok {
		return e, nil
	}
	return nil, nil
}

func (s *MemoryStore) Put(_ context.Context, key string, e *Entry, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[key] = expiring[*Entry]{e, s.now().Add(ttl)}
	delete(s.failures, key)
	return nil
}

func (s *MemoryStore) Failure(_ context.Context, key string) (*Failure, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f, ok := get(s.failures, key, s.now()); ok {
		return f, nil
	}
	return nil, nil
}

func (s *MemoryStore) SetFailure(_ context.Context, key string, f *Failure, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures[key] = expiring[*Failure]{f, s.now().Add(ttl)}
	return nil
}

func (s *MemoryStore) Lock(_ context.Context, key, owner string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := get(s.locks, key, s.now()); ok {
		return false, nil
	}
	s.locks[key] = expiring[string]{owner, s.now().Add(ttl)}
	return true, nil
}

func (s *MemoryStore) Unlock(_ context.Context, key, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o, ok := get(s.locks, key, s.now()); ok && o == owner {
		delete(s.locks, key)
	}
	return nil
}

func (s *MemoryStore) Extend(_ context.Context, key, owner string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if o, ok := get(s.locks, key, s.now()); ok && o == owner {
		s.locks[key] = expiring[string]{owner, s.now().Add(ttl)}
		return true, nil
	}
	return false, nil
}

func (s *MemoryStore) Locked(_ context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := get(s.locks, key, s.now())
	return ok, nil
}

func (s *MemoryStore) Track(_ context.Context, key string, r source.Request, at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tracked[key] = Tracked{Key: key, Request: r, LastAccess: at}
	return nil
}

func (s *MemoryStore) Tracked(_ context.Context, since time.Time) ([]Tracked, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Tracked
	for _, t := range s.tracked {
		if !t.LastAccess.Before(since) {
			out = append(out, t)
		}
	}
	return out, nil
}

func (s *MemoryStore) Untrack(_ context.Context, before time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, t := range s.tracked {
		if t.LastAccess.Before(before) {
			delete(s.tracked, k)
			delete(s.entries, k)
		}
	}
	return nil
}

func (s *MemoryStore) Forget(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tracked, key)
	return nil
}

func (s *MemoryStore) Ping(context.Context) error { return nil }
