package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func lookup(vars map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := vars[k]; return v, ok }
}

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestExampleConfigLoads(t *testing.T) {
	c, err := Load("../../config.example.yml", lookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.Sources["cvedb"].URL != "https://cvedb.shodan.io" || c.Sources["nvd"].Timeout.D() != 3*time.Minute {
		t.Fatalf("sources: %+v %+v", c.Sources["cvedb"], c.Sources["nvd"])
	}
	if got := c.WarmupTargets(); len(got) != 2 || got[0] != "/cvedb/cves?product=go&limit=1000" {
		t.Fatalf("warmup: %v", got)
	}
}

func TestPrecedenceDefaultsFileEnv(t *testing.T) {
	p := write(t, `
cache:
  ttl: 2h
  max_stale: 2d
upstream:
  max_body: 8MiB
sources:
  api:
    url: https://api.example
    timeout: 30s
`)
	c, err := Load(p, lookup(map[string]string{"CACHE_TTL": "3h", "API_TIMEOUT": "45s", "API_RATE": "10/1s"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Cache.TTL.D() != 3*time.Hour || c.Cache.MaxStale.D() != 48*time.Hour || c.Cache.IdleTTL.D() != 24*time.Hour {
		t.Fatalf("cache: %+v", c.Cache)
	}
	if c.Upstream.MaxBody != 8<<20 || c.Upstream.Timeout.D() != 2*time.Minute {
		t.Fatalf("upstream: %+v", c.Upstream)
	}
	if s := c.Sources["api"]; s.Timeout.D() != 45*time.Second || s.Rate != "10/1s" {
		t.Fatalf("source: %+v", s)
	}
}

// Errors point into the original file, comments included.
func TestUnknownKeysAndBadValuesHaveLines(t *testing.T) {
	p := write(t, `# comment
# comment
sources:
  api:
    url: https://api.example
    # comment
    timout: 1m
cahce:
  ttl: 1h
`)
	_, err := Load(p, lookup(nil))
	if err == nil || !strings.Contains(err.Error(), "line 7: unknown key sources.api.timout") ||
		!strings.Contains(err.Error(), "line 8: unknown key cahce") {
		t.Fatalf("err = %v", err)
	}
	p = write(t, "# c\n\nsources:\n  api:\n    url: https://api.example\n    ttl: soon\n")
	if _, err := Load(p, lookup(nil)); err == nil || !strings.Contains(err.Error(), "line 6:") {
		t.Fatalf("err = %v", err)
	}
}

func TestVariables(t *testing.T) {
	p := write(t, `
# ${NOT_REQUIRED} in a comment is not expanded
sources:
  api:
    url: https://${HOST}
    auth: {header: Authorization, value: "Bearer ${TOKEN}"}
    warmup: ["/odata?$filter=x"]
`)
	c, err := Load(p, lookup(map[string]string{"HOST": "api.example", "TOKEN": "t0k"}))
	if err != nil {
		t.Fatal(err)
	}
	s := c.Sources["api"]
	if s.URL != "https://api.example" || s.Auth.Value != "Bearer t0k" || s.Warmup[0] != "/odata?$filter=x" {
		t.Fatalf("%+v", s)
	}
	if _, err := Load(p, lookup(nil)); err == nil || !strings.Contains(err.Error(), "undefined variables: HOST, TOKEN") {
		t.Fatalf("err = %v", err)
	}
}

func TestSourcesSelection(t *testing.T) {
	p := write(t, `
sources:
  a: {url: https://a.example}
  b: {url: https://b.example}
  off: {url: https://off.example, enabled: false}
`)
	c, err := Load(p, lookup(nil))
	if err != nil || !c.Sources["a"].IsEnabled() || c.Sources["off"].IsEnabled() {
		t.Fatalf("file only: %v", err)
	}
	// SOURCES is the exact list; a source missing from the file comes from ENV alone.
	c, err = Load(p, lookup(map[string]string{"SOURCES": "a,env-only", "ENV_ONLY_URL": "https://e.example"}))
	if err != nil {
		t.Fatal(err)
	}
	if !c.Sources["a"].IsEnabled() || c.Sources["b"].IsEnabled() || c.Sources["env-only"].URL != "https://e.example" {
		t.Fatalf("selection: a=%v b=%v env=%+v", c.Sources["a"].IsEnabled(), c.Sources["b"].IsEnabled(), c.Sources["env-only"])
	}
}

func TestValidationReportsEverything(t *testing.T) {
	p := write(t, `
log: {format: xml}
sources:
  metrics: {url: https://m.example}
  nourl: {}
  bad: {url: https://b.example, validate: yaml, auth: {value_file: /nonexistent}, warmup: [cves]}
`)
	_, err := Load(p, lookup(map[string]string{"REDIS_DB": "one"}))
	if err == nil {
		t.Fatal("want errors")
	}
	for _, want := range []string{"REDIS_DB", "log.format", "sources.metrics: invalid name", "sources.nourl.url (NOURL_URL)",
		"sources.bad.validate", "sources.bad.auth.value_file", `sources.bad.warmup: "cves" must start with /`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in:\n%v", want, err)
		}
	}
	if _, err := Load("", lookup(nil)); err == nil || !strings.Contains(err.Error(), "no sources") {
		t.Fatalf("no sources: %v", err)
	}
}

func TestWarmupTargets(t *testing.T) {
	p := write(t, `
sources:
  a: {url: https://a.example, warmup: [/x, "/y?z=1"]}
  off: {url: https://o.example, enabled: false, warmup: [/x]}
`)
	c, err := Load(p, lookup(map[string]string{"CACHE_WARMUP": "/a/global"}))
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Join(c.WarmupTargets(), " ")
	if got != "/a/global /a/x /a/y?z=1" {
		t.Fatalf("got %q", got)
	}
}

func TestEmptyFileIsDefaults(t *testing.T) {
	p := write(t, "# nothing yet\n")
	c, err := Load(p, lookup(map[string]string{"SOURCES": "a", "A_URL": "https://a.example"}))
	if err != nil || c.Listen != ":8080" || c.Cache.TTL.D() != time.Hour {
		t.Fatalf("%v %+v", err, c)
	}
}

// The limits on an open-ended key space have no "unlimited" setting.
func TestLimits(t *testing.T) {
	p := write(t, `
cache: {max_keys: 50, local_max_bytes: 32MiB}
sources:
  a: {url: https://a.example, max_keys: 10}
  b: {url: https://b.example}
`)
	c, err := Load(p, lookup(map[string]string{"UPSTREAM_MAX_IN_FLIGHT": "4", "B_MAX_KEYS": "7"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.Cache.MaxKeys != 50 || c.Cache.LocalMaxBytes != 32<<20 || c.Upstream.MaxInFlight != 4 ||
		c.Sources["a"].MaxKeys != 10 || c.Sources["b"].MaxKeys != 7 {
		t.Fatalf("%+v %+v a=%+v b=%+v", c.Cache, c.Upstream, c.Sources["a"], c.Sources["b"])
	}
	d := Defaults()
	if d.Cache.MaxKeys != 200 || d.Cache.LocalMaxBytes != 64<<20 || d.Upstream.MaxInFlight != 8 {
		t.Fatalf("defaults: %+v %+v", d.Cache, d.Upstream)
	}
	_, err = Load(p, lookup(map[string]string{"CACHE_MAX_KEYS": "0", "CACHE_LOCAL_MAX_BYTES": "0", "UPSTREAM_MAX_IN_FLIGHT": "-1", "A_MAX_KEYS": "-1"}))
	for _, want := range []string{"cache.max_keys", "cache.local_max_bytes", "upstream.max_in_flight", "sources.a.max_keys"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in: %v", want, err)
		}
	}
}
