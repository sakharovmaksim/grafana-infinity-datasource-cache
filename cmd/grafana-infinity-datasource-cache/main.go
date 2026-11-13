package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"syscall"
	"time"
	"uuid"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/cache"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/config"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/metrics"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/server"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/source"
)

// Set at build time: -ldflags "-X main.version=1.2.3 -X main.revision=<sha>".
var (
	version  = "dev"
	revision = "unknown"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "grafana-infinity-datasource-cache:", err)
		os.Exit(1)
	}
}

// defaultConfig is read when present and neither -config nor CONFIG_FILE is given.
const defaultConfig = "config.yml"

func run() error {
	path := flag.String("config", "", "YAML config file (CONFIG_FILE; default ./"+defaultConfig+" if present)")
	check := flag.Bool("check", false, "validate the configuration, print the effective sources and exit")
	flag.Parse()
	file := configPath(*path)
	cfg, err := config.Load(file, os.LookupEnv)
	if err != nil {
		return err
	}
	sources, err := source.Build(cfg)
	if err != nil {
		return err
	}
	if *check {
		_, err := parseWarmup(cfg.WarmupTargets(), sources)
		printEffective(file, cfg, sources)
		return err
	}
	log := newLogger(cfg)
	slog.SetDefault(log)
	source.UserAgent = source.UserAgent + "/" + version + " (+https://github.com/sakharovmaksim/grafana-infinity-datasource-cache)"
	publishConfig(cfg, sources)
	log.Info("config", "file", cmp.Or(file, "none, ENV only"))

	warmup, err := parseWarmup(cfg.WarmupTargets(), sources)
	if err != nil {
		return err
	}

	store, err := newStore(cfg, log)
	if err != nil {
		return err
	}
	if rs, ok := store.(*cache.RedisStore); ok {
		metrics.Registry.MustRegister(rs.Collector())
	}
	host, _ := os.Hostname()
	c := cache.New(store, sources, cache.Options{
		MaxStale:        cfg.Cache.MaxStale.D(),
		IdleTTL:         cfg.Cache.IdleTTL.D(),
		RefreshInterval: cfg.Cache.RefreshInterval.D(),
		ErrorBackoff:    cfg.Cache.ErrorBackoff.D(),
		MissWait:        cfg.Cache.MissWait.D(),
		LocalCheck:      cfg.Cache.LocalCheck.D(),
		LocalMaxBytes:   int64(cfg.Cache.LocalMaxBytes),
		Warmup:          warmup,
		Owner:           host + "-" + uuid.New().String(),
		EntryMetrics:    cfg.Metrics.EntryLimit,
	}, log)
	metrics.Registry.MustRegister(c)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go c.Run(ctx)

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           server.New(c, sources, log),
		ReadHeaderTimeout: 10 * time.Second,
		// Request bodies are at most 1 MiB. No WriteTimeout: it would also cap the wait for a cold
		// fetch; the handler sets a write deadline once the data is ready instead.
		ReadTimeout: 30 * time.Second,
		IdleTimeout: 2 * time.Minute,
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	for name, s := range sources {
		log.Info("source", "name", name, "url", s.URL.String(), "paths", s.Paths, "methods", s.Methods,
			"ttl", s.TTL.String(), "concurrency", s.Concurrency, "max_keys", s.MaxKeys)
	}
	log.Info("listening", "addr", cfg.Listen, "version", version, "warmup", len(warmup))

	select {
	case err := <-errc:
		c.Close(5 * time.Second)
		return err
	case <-ctx.Done():
	}
	// Let quick requests finish, then abort background fetches so their locks are released
	// for the other replicas right away instead of after a lease.
	log.Info("shutting down", "cause", context.Cause(ctx))
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(sctx)
	c.Close(5 * time.Second)
	if errors.Is(err, context.DeadlineExceeded) {
		log.Warn("requests still waiting for upstream at shutdown")
		return nil
	}
	return err
}

// publishConfig exposes settings as metrics so alert rules compare against deployed values.
func publishConfig(cfg *config.Config, sources map[string]*source.Source) {
	metrics.BuildInfo.WithLabelValues(version, revision, runtime.Version()).Set(1)
	metrics.ConfigMaxStale.Set(cfg.Cache.MaxStale.D().Seconds())
	metrics.ConfigIdleTTL.Set(cfg.Cache.IdleTTL.D().Seconds())
	metrics.ConfigErrorBackoff.Set(cfg.Cache.ErrorBackoff.D().Seconds())
	metrics.ConfigRefreshInterval.Set(cfg.Cache.RefreshInterval.D().Seconds())
	metrics.ConfigMissWait.Set(cfg.Cache.MissWait.D().Seconds())
	metrics.ConfigLocalMaxBytes.Set(float64(cfg.Cache.LocalMaxBytes))
	for name, s := range sources {
		metrics.SourceTTL.WithLabelValues(name).Set(s.TTL.Seconds())
		metrics.SourceTimeout.WithLabelValues(name).Set(s.Timeout.Seconds())
		metrics.SourceMissWait.WithLabelValues(name).Set(s.MissWait.Seconds())
		metrics.SourceConcurrency.WithLabelValues(name).Set(float64(s.Concurrency))
		metrics.SourceMaxKeys.WithLabelValues(name).Set(float64(s.MaxKeys))
		metrics.SourceMaxInFlight.WithLabelValues(name).Set(float64(cap(s.Slots)))
		rps := 0.0
		if s.Limiter != nil {
			rps = float64(s.Limiter.Limit())
		}
		metrics.SourceRateLimit.WithLabelValues(name).Set(rps)
	}
}

// printEffective shows what the service would run with, defaults applied and secrets hidden.
func printEffective(file string, cfg *config.Config, sources map[string]*source.Source) {
	fmt.Printf("config: %s\nredis: %q db=%d prefix=%q\ncache: ttl=%v max_stale=%v idle_ttl=%v miss_wait=%v local_max_bytes=%d\nwarmup:\n",
		cmp.Or(file, "none, ENV only"), cfg.Redis.Addr, cfg.Redis.DB, cfg.Redis.KeyPrefix,
		cfg.Cache.TTL.D(), cfg.Cache.MaxStale.D(), cfg.Cache.IdleTTL.D(), cfg.Cache.MissWait.D(),
		cfg.Cache.LocalMaxBytes)
	for _, w := range cfg.WarmupTargets() {
		fmt.Printf("  %s\n", w)
	}
	for _, name := range slices.Sorted(maps.Keys(sources)) {
		s, sc := sources[name], cfg.Sources[name]
		rate := "unlimited"
		if s.Limiter != nil {
			rate = fmt.Sprintf("%.3g/s", float64(s.Limiter.Limit()))
		}
		auth := "none"
		switch {
		case sc.Auth.Value != "":
			auth = s.AuthHeader + ": <value>"
		case sc.Auth.ValueFile != "":
			auth = s.AuthHeader + ": <" + sc.Auth.ValueFile + ">"
		}
		fmt.Printf("source %s: url=%s paths=%v methods=%v ttl=%v timeout=%v miss_wait=%v attempts=%d concurrency=%d max_in_flight=%d max_keys=%d rate=%s validate_json=%v max_body=%d auth=%s\n",
			name, s.URL, s.Paths, s.Methods, s.TTL, s.Timeout, s.MissWait, s.Attempts, s.Concurrency, cap(s.Slots), s.MaxKeys, rate, s.ValidateJSON, s.MaxBodyBytes, auth)
	}
}

// configPath picks the flag, then CONFIG_FILE, then ./config.yml if it exists; "" means ENV only.
func configPath(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	if v := os.Getenv("CONFIG_FILE"); v != "" {
		return v
	}
	if _, err := os.Stat(defaultConfig); err == nil {
		return defaultConfig
	}
	return ""
}

func newLogger(cfg *config.Config) *slog.Logger {
	var level slog.Level
	_ = level.UnmarshalText([]byte(cfg.Log.Level)) // validated by config.Load
	opts := &slog.HandlerOptions{Level: level}
	if cfg.Log.Format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stdout, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stdout, opts))
}

// newStore never fails on an unreachable Redis: replicas serve from memory until it is back.
func newStore(cfg *config.Config, log *slog.Logger) (cache.Store, error) {
	r := cfg.Redis
	if r.Addr == "" {
		log.Warn("redis.addr is empty: in-memory store, nothing is shared or kept across restarts")
		return cache.NewMemoryStore(), nil
	}
	password, err := config.SecretGetter(r.Password, r.PasswordFile)()
	if err != nil {
		return nil, fmt.Errorf("redis.password_file: %w", err)
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:        r.Addr,
		DB:          r.DB,
		Password:    password,
		DialTimeout: 2 * time.Second,
		// One in-cluster instance: retries only add latency, the replica serves from memory instead.
		MaxRetries:      1,
		DialerRetries:   2,
		ReadTimeout:     3 * time.Second,
		WriteTimeout:    3 * time.Second,
		DisableIdentity: true,
		// Standalone Redis has no maintenance notifications; auto mode only logs handshake errors.
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	// go-redis prints every failed dial in plain text; the cache already logs outages, throttled, in JSON.
	redis.SetLogger(redisLogger{log})
	store := cache.NewRedisStore(rdb, r.KeyPrefix)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := store.Ping(ctx); err != nil {
		log.Warn("redis is unreachable, starting anyway", "addr", r.Addr, "error", err)
	} else {
		log.Info("redis connected", "addr", r.Addr, "db", r.DB, "prefix", r.KeyPrefix)
	}
	return store, nil
}

type redisLogger struct{ log *slog.Logger }

func (l redisLogger) Printf(ctx context.Context, format string, v ...any) {
	l.log.DebugContext(ctx, "redis client", "detail", fmt.Sprintf(format, v...))
}

func parseWarmup(targets []string, sources map[string]*source.Source) ([]source.Request, error) {
	out := make([]source.Request, 0, len(targets))
	for _, t := range targets {
		name, path, query, err := source.ParseTarget(t)
		if err != nil {
			return nil, fmt.Errorf("CACHE_WARMUP: %w", err)
		}
		src, ok := sources[name]
		if !ok {
			return nil, fmt.Errorf("CACHE_WARMUP: %q: source %q is not in SOURCES", t, name)
		}
		if !src.Allows(http.MethodGet, path) {
			return nil, fmt.Errorf("CACHE_WARMUP: %q: path is not allowed for source %q", t, name)
		}
		out = append(out, source.NewRequest(name, http.MethodGet, path, query, "", nil))
	}
	return out, nil
}
