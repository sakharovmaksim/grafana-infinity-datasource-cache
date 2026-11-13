package cache

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/metrics"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/source"
)

// RedisStore layout, all keys under one prefix:
//
//	entry:<key>  hash {meta, body}, expires after CACHE_MAX_STALE or when the key goes idle
//	fail:<key>   last failure, expires after CACHE_ERROR_BACKOFF
//	lock:<key>   owner of the running fetch
//	index        zset key → last access (unix seconds)
//	requests     hash key → request, to replay it on refresh
type RedisStore struct {
	rdb    redis.UniversalClient
	prefix string
}

func NewRedisStore(rdb redis.UniversalClient, prefix string) *RedisStore {
	return &RedisStore{rdb: rdb, prefix: prefix}
}

func (s *RedisStore) k(kind, key string) string { return s.prefix + kind + ":" + key }

var (
	unlockScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) end
return 0`)
	extendScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) end
return 0`)
)

func (s *RedisStore) Meta(ctx context.Context, key string) (*Meta, error) {
	b, err := s.rdb.HGet(ctx, s.k("entry", key), "meta").Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var m Meta
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("decode meta: %w", err)
	}
	return &m, nil
}

// Get reads meta and body in one HMGET so they always belong to the same fetch.
func (s *RedisStore) Get(ctx context.Context, key string) (*Entry, error) {
	vals, err := s.rdb.HMGet(ctx, s.k("entry", key), "meta", "body").Result()
	if err != nil {
		return nil, err
	}
	meta, ok1 := vals[0].(string)
	body, ok2 := vals[1].(string)
	if !ok1 || !ok2 {
		return nil, nil
	}
	e := &Entry{Gzip: []byte(body)}
	if err := json.Unmarshal([]byte(meta), &e.Meta); err != nil {
		return nil, fmt.Errorf("decode meta: %w", err)
	}
	return e, nil
}

func (s *RedisStore) Put(ctx context.Context, key string, e *Entry, ttl time.Duration) error {
	meta, err := json.Marshal(e.Meta)
	if err != nil {
		return err
	}
	_, err = s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, s.k("entry", key), "meta", meta, "body", e.Gzip)
		p.PExpire(ctx, s.k("entry", key), ttl)
		p.Del(ctx, s.k("fail", key))
		return nil
	})
	return err
}

func (s *RedisStore) Failure(ctx context.Context, key string) (*Failure, error) {
	b, err := s.rdb.Get(ctx, s.k("fail", key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var f Failure
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, err
	}
	return &f, nil
}

func (s *RedisStore) SetFailure(ctx context.Context, key string, f *Failure, ttl time.Duration) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, s.k("fail", key), b, ttl).Err()
}

func (s *RedisStore) Lock(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return s.rdb.SetNX(ctx, s.k("lock", key), owner, ttl).Result()
}

func (s *RedisStore) Unlock(ctx context.Context, key, owner string) error {
	return unlockScript.Run(ctx, s.rdb, []string{s.k("lock", key)}, owner).Err()
}

func (s *RedisStore) Extend(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	n, err := extendScript.Run(ctx, s.rdb, []string{s.k("lock", key)}, owner, ttl.Milliseconds()).Int()
	return n == 1, err
}

func (s *RedisStore) Locked(ctx context.Context, key string) (bool, error) {
	n, err := s.rdb.Exists(ctx, s.k("lock", key)).Result()
	return n > 0, err
}

func (s *RedisStore) Track(ctx context.Context, key string, r source.Request, at time.Time) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	_, err = s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, s.prefix+"requests", key, b)
		p.ZAdd(ctx, s.prefix+"index", redis.Z{Score: float64(at.Unix()), Member: key})
		return nil
	})
	return err
}

func (s *RedisStore) Tracked(ctx context.Context, since time.Time) ([]Tracked, error) {
	zs, err := s.rdb.ZRangeByScoreWithScores(ctx, s.prefix+"index", &redis.ZRangeBy{
		Min: strconv.FormatInt(since.Unix(), 10), Max: "+inf",
	}).Result()
	if err != nil || len(zs) == 0 {
		return nil, err
	}
	keys := make([]string, len(zs))
	for i, z := range zs {
		keys[i] = z.Member.(string)
	}
	reqs, err := s.rdb.HMGet(ctx, s.prefix+"requests", keys...).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Tracked, 0, len(zs))
	for i, v := range reqs {
		raw, ok := v.(string)
		if !ok {
			continue // pruned concurrently; the next Track restores it
		}
		t := Tracked{Key: keys[i], LastAccess: time.Unix(int64(zs[i].Score), 0)}
		if json.Unmarshal([]byte(raw), &t.Request) == nil {
			out = append(out, t)
		}
	}
	return out, nil
}

func (s *RedisStore) Untrack(ctx context.Context, before time.Time) error {
	max := "(" + strconv.FormatInt(before.Unix(), 10)
	keys, err := s.rdb.ZRangeByScore(ctx, s.prefix+"index", &redis.ZRangeBy{Min: "-inf", Max: max}).Result()
	if err != nil || len(keys) == 0 {
		return err
	}
	entries := make([]string, len(keys))
	for i, k := range keys {
		entries[i] = s.k("entry", k)
	}
	_, err = s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HDel(ctx, s.prefix+"requests", keys...)
		p.ZRemRangeByScore(ctx, s.prefix+"index", "-inf", max)
		p.Del(ctx, entries...)
		return nil
	})
	return err
}

func (s *RedisStore) Forget(ctx context.Context, key string) error {
	_, err := s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZRem(ctx, s.prefix+"index", key)
		p.HDel(ctx, s.prefix+"requests", key)
		return nil
	})
	return err
}

func (s *RedisStore) Ping(ctx context.Context) error { return s.rdb.Ping(ctx).Err() }

// Collector exposes connection pool counters of the Redis client.
func (s *RedisStore) Collector() prometheus.Collector { return poolCollector{s.rdb} }

type poolCollector struct{ rdb redis.UniversalClient }

func (p poolCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		metrics.RedisPoolHitsDesc, metrics.RedisPoolMissesDesc, metrics.RedisPoolTimeoutsDesc,
		metrics.RedisPoolWaitsDesc, metrics.RedisPoolWaitSecondsDesc, metrics.RedisPoolConnsDesc, metrics.RedisPoolStaleDesc,
	} {
		ch <- d
	}
}

func (p poolCollector) Collect(ch chan<- prometheus.Metric) {
	st := p.rdb.PoolStats()
	counter := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v)
	}
	counter(metrics.RedisPoolHitsDesc, float64(st.Hits))
	counter(metrics.RedisPoolMissesDesc, float64(st.Misses))
	counter(metrics.RedisPoolTimeoutsDesc, float64(st.Timeouts))
	counter(metrics.RedisPoolWaitsDesc, float64(st.WaitCount))
	counter(metrics.RedisPoolWaitSecondsDesc, time.Duration(st.WaitDurationNs).Seconds())
	counter(metrics.RedisPoolStaleDesc, float64(st.StaleConns))
	ch <- prometheus.MustNewConstMetric(metrics.RedisPoolConnsDesc, prometheus.GaugeValue, float64(st.TotalConns), "total")
	ch <- prometheus.MustNewConstMetric(metrics.RedisPoolConnsDesc, prometheus.GaugeValue, float64(st.IdleConns), "idle")
}
