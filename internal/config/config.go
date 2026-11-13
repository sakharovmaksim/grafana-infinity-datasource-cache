package config

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Config is the whole service configuration: built-in defaults, overridden by the YAML file,
// overridden by ENV. Nothing about particular upstreams is built in: sources come only from here.
type Config struct {
	Listen   string                   `yaml:"listen"`
	Log      Log                      `yaml:"log"`
	Redis    Redis                    `yaml:"redis"`
	Cache    Cache                    `yaml:"cache"`
	Upstream Upstream                 `yaml:"upstream"`
	Metrics  Metrics                  `yaml:"metrics"`
	Sources  map[string]*SourceConfig `yaml:"sources"`

	// Warmup comes from CACHE_WARMUP as "/<source><path>"; per-source lists are in SourceConfig.Warmup.
	Warmup []string `yaml:"-"`
}

type Log struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"` // text | json
}

type Redis struct {
	Addr         string `yaml:"addr"` // empty = in-memory store, for local runs
	DB           int    `yaml:"db"`
	KeyPrefix    string `yaml:"key_prefix"`
	Password     string `yaml:"password"`
	PasswordFile string `yaml:"password_file"`
}

type Cache struct {
	TTL             Duration `yaml:"ttl"`              // data younger than this is served without refresh
	MaxStale        Duration `yaml:"max_stale"`        // stale data is served while upstream fails, but no longer than this
	IdleTTL         Duration `yaml:"idle_ttl"`         // keys nobody requested for this long stop being refreshed
	RefreshInterval Duration `yaml:"refresh_interval"` //
	ErrorBackoff    Duration `yaml:"error_backoff"`    // pause after a failed fetch, shared by all replicas
	MissWait        Duration `yaml:"miss_wait"`        // how long a cold request waits for the upstream
	LocalCheck      Duration `yaml:"local_check"`      // how often a replica checks Redis for a newer copy
	// Bounds for an open-ended key space: every distinct URL is a key, and a URL with a time range
	// (${__from} in Infinity) is a new key on every dashboard refresh.
	MaxKeys       int  `yaml:"max_keys"`        // per-source default: tracked keys besides warmup; new ones above it bypass the cache
	LocalMaxBytes Size `yaml:"local_max_bytes"` // replica copies; least recently used are dropped and reread from Redis
}

type Upstream struct {
	Timeout     Duration `yaml:"timeout"`
	MaxBody     Size     `yaml:"max_body"`
	MaxInFlight int      `yaml:"max_in_flight"` // per-source default: upstream requests at once, per replica
}

type Metrics struct {
	EntryLimit int `yaml:"entry_limit"` // per-entry series cap, 0 = none
}

// SourceConfig describes one upstream. Zero values fall back to the global settings or built-in defaults.
type SourceConfig struct {
	Enabled     *bool    `yaml:"enabled"`
	URL         string   `yaml:"url"`
	Paths       []string `yaml:"paths"`   // allowed path prefixes, default "/"
	Methods     []string `yaml:"methods"` // default GET
	TTL         Duration `yaml:"ttl"`
	Timeout     Duration `yaml:"timeout"`   // per attempt
	MissWait    Duration `yaml:"miss_wait"` // default cache.miss_wait; keep under the datasource timeout
	Attempts    int      `yaml:"attempts"`
	Concurrency int      `yaml:"concurrency"`
	MaxKeys     int      `yaml:"max_keys"`      // default cache.max_keys
	MaxInFlight int      `yaml:"max_in_flight"` // default upstream.max_in_flight
	Rate        string   `yaml:"rate"`          // "50/30s", empty or "0" = unlimited
	Validate    string   `yaml:"validate"`      // json | none
	MaxBody     Size     `yaml:"max_body"`
	Auth        Auth     `yaml:"auth"`
	Warmup      []string `yaml:"warmup"` // paths of this source, GET only
}

type Auth struct {
	Header    string `yaml:"header"`
	Value     string `yaml:"value"`
	ValueFile string `yaml:"value_file"` // re-read on every request: a rotated Secret applies without restart
}

func (s *SourceConfig) IsEnabled() bool { return s.Enabled == nil || *s.Enabled }

func Defaults() *Config {
	return &Config{
		Listen:   ":8080",
		Log:      Log{Level: "info", Format: "text"},
		Redis:    Redis{KeyPrefix: "datasource-cache:"},
		Upstream: Upstream{Timeout: Duration(2 * time.Minute), MaxBody: 64 << 20, MaxInFlight: 8},
		Metrics:  Metrics{EntryLimit: 200},
		Cache: Cache{
			TTL:             Duration(time.Hour),
			MaxStale:        Duration(2 * 24 * time.Hour),
			IdleTTL:         Duration(24 * time.Hour),
			RefreshInterval: Duration(time.Minute),
			ErrorBackoff:    Duration(time.Minute),
			MissWait:        Duration(50 * time.Second), // under Infinity's 60 s default timeout
			LocalCheck:      Duration(10 * time.Second),
			MaxKeys:         200,
			LocalMaxBytes:   64 << 20,
		},
		Sources: map[string]*SourceConfig{},
	}
}

// Load reads the YAML file at path (none if empty), applies ENV overrides and validates the result.
// All problems are reported together.
func Load(path string, lookup func(string) (string, bool)) (*Config, error) {
	c := Defaults()
	if path != "" {
		if err := c.readFile(path, lookup); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
	}
	e := NewEnv(lookup)
	c.applyEnv(e)
	return c, errors.Join(e.Err(), c.validate())
}

// readFile decodes strictly, so a typo in a key fails the start instead of being silently ignored.
// ${VAR} is expanded in values only, not in comments; a bare $ is left alone ($filter in OData queries).
// Everything works on the parsed tree, so error line numbers point into the original file.
func (c *Config) readFile(path string, lookup func(string) (string, bool)) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return err
	}
	if len(doc.Content) == 0 {
		return nil // empty file
	}
	var errs []error
	checkKeys(doc.Content[0], reflect.TypeFor[Config](), "", &errs)
	var missing []string
	expand(&doc, lookup, &missing)
	if len(missing) > 0 {
		slices.Sort(missing)
		errs = append(errs, fmt.Errorf("undefined variables: %s", strings.Join(slices.Compact(missing), ", ")))
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return doc.Decode(c)
}

// checkKeys reports mapping keys that match no yaml tag of t.
func checkKeys(n *yaml.Node, t reflect.Type, path string, errs *[]error) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch {
	case n.Kind == yaml.MappingNode && t.Kind() == reflect.Struct:
		for i := 0; i+1 < len(n.Content); i += 2 {
			key := n.Content[i]
			f, ok := yamlField(t, key.Value)
			if !ok {
				*errs = append(*errs, fmt.Errorf("line %d: unknown key %s%s", key.Line, path, key.Value))
				continue
			}
			checkKeys(n.Content[i+1], f.Type, path+key.Value+".", errs)
		}
	case n.Kind == yaml.MappingNode && t.Kind() == reflect.Map:
		for i := 0; i+1 < len(n.Content); i += 2 {
			checkKeys(n.Content[i+1], t.Elem(), path+n.Content[i].Value+".", errs)
		}
	case n.Kind == yaml.SequenceNode && t.Kind() == reflect.Slice:
		for _, item := range n.Content {
			checkKeys(item, t.Elem(), path, errs)
		}
	}
}

func yamlField(t reflect.Type, key string) (reflect.StructField, bool) {
	for f := range t.Fields() {
		if name, _, _ := strings.Cut(f.Tag.Get("yaml"), ","); name == key && name != "-" {
			return f, true
		}
	}
	return reflect.StructField{}, false
}

var varRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

func expand(n *yaml.Node, lookup func(string) (string, bool), missing *[]string) {
	if n.Kind == yaml.ScalarNode && strings.Contains(n.Value, "${") {
		n.Value = varRe.ReplaceAllStringFunc(n.Value, func(m string) string {
			name := m[2 : len(m)-1]
			v, ok := lookup(name)
			if !ok {
				*missing = append(*missing, name)
			}
			return v
		})
	}
	for _, child := range n.Content {
		expand(child, lookup, missing)
	}
}

func (c *Config) applyEnv(e *Env) {
	c.Listen = e.String("LISTEN_ADDR", c.Listen)
	c.Log.Level = e.String("LOG_LEVEL", c.Log.Level)
	c.Log.Format = e.String("LOG_FORMAT", c.Log.Format)

	c.Redis.Addr = e.String("REDIS_ADDR", c.Redis.Addr)
	c.Redis.DB = e.Int("REDIS_DB", c.Redis.DB)
	c.Redis.KeyPrefix = e.String("REDIS_KEY_PREFIX", c.Redis.KeyPrefix)
	c.Redis.Password = e.String("REDIS_PASSWORD", c.Redis.Password)
	c.Redis.PasswordFile = e.String("REDIS_PASSWORD_FILE", c.Redis.PasswordFile)

	c.Cache.TTL = e.Parse("CACHE_TTL", c.Cache.TTL, ParseDurationValue)
	c.Cache.MaxStale = e.Parse("CACHE_MAX_STALE", c.Cache.MaxStale, ParseDurationValue)
	c.Cache.IdleTTL = e.Parse("CACHE_IDLE_TTL", c.Cache.IdleTTL, ParseDurationValue)
	c.Cache.RefreshInterval = e.Parse("CACHE_REFRESH_INTERVAL", c.Cache.RefreshInterval, ParseDurationValue)
	c.Cache.ErrorBackoff = e.Parse("CACHE_ERROR_BACKOFF", c.Cache.ErrorBackoff, ParseDurationValue)
	c.Cache.MissWait = e.Parse("CACHE_MISS_WAIT", c.Cache.MissWait, ParseDurationValue)
	c.Cache.LocalCheck = e.Parse("CACHE_LOCAL_CHECK", c.Cache.LocalCheck, ParseDurationValue)
	c.Cache.MaxKeys = e.Int("CACHE_MAX_KEYS", c.Cache.MaxKeys)
	c.Cache.LocalMaxBytes = e.Parse("CACHE_LOCAL_MAX_BYTES", c.Cache.LocalMaxBytes, ParseSize)

	c.Upstream.Timeout = e.Parse("UPSTREAM_TIMEOUT", c.Upstream.Timeout, ParseDurationValue)
	c.Upstream.MaxBody = e.Parse("UPSTREAM_MAX_BODY", c.Upstream.MaxBody, ParseSize)
	c.Upstream.MaxInFlight = e.Int("UPSTREAM_MAX_IN_FLIGHT", c.Upstream.MaxInFlight)
	c.Metrics.EntryLimit = e.Int("METRICS_ENTRY_LIMIT", c.Metrics.EntryLimit)
	c.Warmup = e.List("CACHE_WARMUP", nil)

	if c.Sources == nil {
		c.Sources = map[string]*SourceConfig{}
	}
	// SOURCES, when set, is the exact list of enabled sources; ones missing from the file come from ENV alone.
	if names := e.Names("SOURCES", nil); names != nil {
		for name, s := range c.Sources {
			s.Enabled = new(slices.Contains(names, name))
		}
		for _, name := range names {
			if c.Sources[name] == nil {
				c.Sources[name] = &SourceConfig{}
			}
		}
	}
	for name, s := range c.Sources {
		if s == nil {
			s = &SourceConfig{}
			c.Sources[name] = s
		}
		s.applyEnv(e, EnvPrefix(name))
	}
}

func (s *SourceConfig) applyEnv(e *Env, p string) {
	s.URL = e.String(p+"URL", s.URL)
	s.Paths = e.List(p+"PATHS", s.Paths)
	s.Methods = e.Names(p+"METHODS", s.Methods)
	s.TTL = e.Parse(p+"TTL", s.TTL, ParseDurationValue)
	s.Timeout = e.Parse(p+"TIMEOUT", s.Timeout, ParseDurationValue)
	s.MissWait = e.Parse(p+"MISS_WAIT", s.MissWait, ParseDurationValue)
	s.Attempts = e.Int(p+"ATTEMPTS", s.Attempts)
	s.Concurrency = e.Int(p+"CONCURRENCY", s.Concurrency)
	s.MaxKeys = e.Int(p+"MAX_KEYS", s.MaxKeys)
	s.MaxInFlight = e.Int(p+"MAX_IN_FLIGHT", s.MaxInFlight)
	s.Rate = e.String(p+"RATE", s.Rate)
	s.Validate = e.String(p+"VALIDATE", s.Validate)
	s.MaxBody = e.Parse(p+"MAX_BODY", s.MaxBody, ParseSize)
	s.Auth.Header = e.String(p+"AUTH_HEADER", s.Auth.Header)
	s.Auth.Value = e.String(p+"AUTH_VALUE", s.Auth.Value)
	s.Auth.ValueFile = e.String(p+"AUTH_VALUE_FILE", s.Auth.ValueFile)
	s.Warmup = e.List(p+"WARMUP", s.Warmup)
}

// EnvPrefix is the ENV prefix of a source: "my-api" → "MY_API_".
func EnvPrefix(name string) string {
	return strings.ToUpper(strings.ReplaceAll(name, "-", "_")) + "_"
}

// Source names are path segments and ENV prefixes; reserved ones collide with service routes.
var (
	nameRe   = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	reserved = map[string]bool{"healthz": true, "metrics": true}
)

func (c *Config) validate() error {
	var errs []error
	fail := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	var level slog.Level
	if err := level.UnmarshalText([]byte(c.Log.Level)); err != nil {
		fail("log.level: %v", err)
	}
	if c.Log.Format != "text" && c.Log.Format != "json" {
		fail("log.format: want text or json, got %q", c.Log.Format)
	}
	// 0 would mean "unbounded" for these, which is exactly what they protect against.
	if c.Cache.MaxKeys <= 0 {
		fail("cache.max_keys: want > 0, got %d", c.Cache.MaxKeys)
	}
	if c.Cache.LocalMaxBytes <= 0 {
		fail("cache.local_max_bytes: want > 0, got %d", c.Cache.LocalMaxBytes)
	}
	if c.Upstream.MaxInFlight <= 0 {
		fail("upstream.max_in_flight: want > 0, got %d", c.Upstream.MaxInFlight)
	}
	if f := c.Redis.PasswordFile; f != "" {
		if _, err := os.Stat(f); err != nil {
			fail("redis.password_file: %v", err)
		}
	}

	enabled := 0
	for _, name := range slices.Sorted(maps.Keys(c.Sources)) {
		s := c.Sources[name]
		if !s.IsEnabled() {
			continue
		}
		enabled++
		if !nameRe.MatchString(name) || reserved[name] {
			fail("sources.%s: invalid name, want [a-z][a-z0-9-]* and not healthz or metrics", name)
		}
		if u, err := url.Parse(s.URL); s.URL == "" || err != nil || u.Scheme == "" || u.Host == "" {
			fail("sources.%s.url (%sURL): want an absolute URL, got %q", name, EnvPrefix(name), s.URL)
		}
		if s.MaxKeys < 0 {
			fail("sources.%s.max_keys: want > 0, got %d", name, s.MaxKeys)
		}
		if s.MaxInFlight < 0 {
			fail("sources.%s.max_in_flight: want > 0, got %d", name, s.MaxInFlight)
		}
		if s.Validate != "" && s.Validate != "json" && s.Validate != "none" {
			fail("sources.%s.validate: want json or none, got %q", name, s.Validate)
		}
		if f := s.Auth.ValueFile; f != "" {
			if _, err := os.Stat(f); err != nil {
				fail("sources.%s.auth.value_file: %v", name, err)
			}
		}
		for _, w := range s.Warmup {
			if !strings.HasPrefix(w, "/") {
				fail("sources.%s.warmup: %q must start with /", name, w)
			}
		}
	}
	if enabled == 0 {
		fail("no sources: describe them under sources in the config file or with SOURCES and <NAME>_URL")
	}
	return errors.Join(errs...)
}

// WarmupTargets lists everything to keep warm as "/<source><path>": CACHE_WARMUP plus per-source lists.
func (c *Config) WarmupTargets() []string {
	out := slices.Clone(c.Warmup)
	for _, name := range slices.Sorted(maps.Keys(c.Sources)) {
		if s := c.Sources[name]; s.IsEnabled() {
			for _, w := range s.Warmup {
				out = append(out, "/"+name+w)
			}
		}
	}
	return out
}

// SecretGetter returns value, or the contents of file re-read on every call.
func SecretGetter(value, file string) func() (string, error) {
	switch {
	case value != "":
		return func() (string, error) { return value, nil }
	case file != "":
		return func() (string, error) {
			b, err := os.ReadFile(file)
			return strings.TrimSpace(string(b)), err
		}
	}
	return func() (string, error) { return "", nil }
}

// Duration is a time.Duration that also accepts days: "7d", "1d12h".
type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	v, err := ParseDurationValue(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*d = v
	return nil
}

func ParseDurationValue(s string) (Duration, error) {
	d, err := ParseDuration(s)
	return Duration(d), err
}

// Size is a byte count that also accepts KiB, MiB and GiB suffixes.
type Size int64

func (s *Size) UnmarshalYAML(n *yaml.Node) error {
	v, err := ParseSize(n.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", n.Line, err)
	}
	*s = v
	return nil
}

func ParseSize(s string) (Size, error) {
	n, err := ParseBytes(s)
	return Size(n), err
}
