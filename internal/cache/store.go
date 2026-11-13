package cache

import (
	"context"
	"time"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/source"
)

// Meta describes a stored response; it is small so replicas can poll it cheaply.
type Meta struct {
	Request     source.Request `json:"request"`
	FetchedAt   time.Time      `json:"fetched_at"`
	ContentType string         `json:"content_type"`
	Size        int64          `json:"size"`        // uncompressed
	StoredSize  int64          `json:"stored_size"` // gzipped, as held in the store
	Hash        string         `json:"hash"`        // sha256 of the uncompressed body
}

// Entry is a stored response; the body is kept gzipped to save shared Redis memory.
type Entry struct {
	Meta Meta
	Gzip []byte
}

// Failure is shared by replicas so a failing upstream is retried once per backoff, not per request.
type Failure struct {
	Msg    string    `json:"msg"`
	Status int       `json:"status,omitzero"`
	Reason string    `json:"reason,omitzero"` // kept so a backed-off timeout is still 504
	At     time.Time `json:"at"`
}

func (f *Failure) Err() error { return &source.Error{Status: f.Status, Msg: f.Msg, Reason: f.Reason} }

type Tracked struct {
	Key        string
	Request    source.Request
	LastAccess time.Time
}

// Store is shared state between replicas. Absent values are returned as nil without error.
type Store interface {
	Meta(ctx context.Context, key string) (*Meta, error)
	Get(ctx context.Context, key string) (*Entry, error)
	Put(ctx context.Context, key string, e *Entry, ttl time.Duration) error

	Failure(ctx context.Context, key string) (*Failure, error)
	SetFailure(ctx context.Context, key string, f *Failure, ttl time.Duration) error

	Lock(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
	Unlock(ctx context.Context, key, owner string) error
	// Extend renews the lease if owner still holds it.
	Extend(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
	Locked(ctx context.Context, key string) (bool, error)

	// Track records an access; tracked keys are refreshed in the background.
	Track(ctx context.Context, key string, r source.Request, at time.Time) error
	Tracked(ctx context.Context, since time.Time) ([]Tracked, error)
	// Untrack drops keys idle since before together with their entries: an entry outliving its key would
	// keep Redis memory (shared with Grafana) after the key freed its max_keys slot.
	Untrack(ctx context.Context, before time.Time) error
	Forget(ctx context.Context, key string) error

	Ping(ctx context.Context) error
}
