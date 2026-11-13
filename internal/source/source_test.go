package source

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/config"
)

func TestCanonicalQueryKeepsValuelessFlags(t *testing.T) {
	got := CanonicalQuery("virtualMatchString=cpe:2.3:a:golang:go&noRejected&&resultsPerPage=2000")
	if want := "&noRejected&resultsPerPage=2000&virtualMatchString=cpe:2.3:a:golang:go"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	a := NewRequest("nvd", "GET", "/x", "b=2&a=1", "", nil)
	b := NewRequest("nvd", "GET", "/x", "a=1&b=2", "", nil)
	if a.Key() != b.Key() {
		t.Fatal("parameter order changes the key")
	}
	plain := NewRequest("nvd", "GET", "/x", "virtualMatchString=cpe:2.3:a:golang:go&noRejected", "", nil)
	encoded := NewRequest("nvd", "GET", "/x", "noRejected&virtualMatchString=cpe%3A2.3%3Aa%3Agolang%3Ago", "", nil)
	if plain.Key() != encoded.Key() {
		t.Fatal("URL encoding changes the key")
	}
	if plain.Query == encoded.Query {
		t.Fatal("the upstream query must stay as the client sent it")
	}
}

func TestParseTarget(t *testing.T) {
	name, path, query, err := ParseTarget("/nvd/rest/json/cves/2.0?noRejected&a=1")
	if err != nil || name != "nvd" || path != "/rest/json/cves/2.0" || query != "noRejected&a=1" {
		t.Fatalf("%q %q %q %v", name, path, query, err)
	}
	if _, _, _, err := ParseTarget("nvd/x"); err == nil {
		t.Fatal("want error without leading slash")
	}
}

func load(t *testing.T, vars map[string]string) *config.Config {
	t.Helper()
	c, err := config.Load("", func(k string) (string, bool) { v, ok := vars[k]; return v, ok })
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestBuildDefaultsAndOverrides(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "key")
	if err := os.WriteFile(keyFile, []byte("k1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := load(t, map[string]string{
		"SOURCES":             "plain,osv",
		"PLAIN_URL":           "https://plain.example",
		"OSV_URL":             "https://api.osv.dev",
		"OSV_PATHS":           "/v1/query /v1/querybatch",
		"OSV_METHODS":         "post",
		"OSV_TTL":             "1d",
		"OSV_MISS_WAIT":       "170s",
		"OSV_RATE":            "50/30s",
		"OSV_ATTEMPTS":        "1",
		"OSV_MAX_BODY":        "1MiB",
		"OSV_VALIDATE":        "none",
		"OSV_AUTH_HEADER":     "apiKey",
		"OSV_AUTH_VALUE_FILE": keyFile,
		"CACHE_TTL":           "2h",
		"UPSTREAM_TIMEOUT":    "40s",
	})
	srcs, err := Build(c)
	if err != nil {
		t.Fatal(err)
	}
	// Nothing about an upstream is built in: unset fields fall back to global settings and generic defaults.
	p := srcs["plain"]
	if !p.Allows("GET", "/anything") || p.Allows("POST", "/x") || p.TTL != 2*time.Hour || p.Timeout != 40*time.Second ||
		p.Attempts != 2 || p.Concurrency != 4 || p.Limiter != nil || !p.ValidateJSON || p.MaxBodyBytes != 64<<20 || p.AuthHeader != "" {
		t.Fatalf("defaults: %+v", p)
	}
	o := srcs["osv"]
	if !o.Allows("POST", "/v1/query") || o.Allows("GET", "/v1/query") || o.Allows("POST", "/v1/vulns") {
		t.Fatal("osv allow rules")
	}
	if p.MissWait != 50*time.Second || o.MissWait != 170*time.Second {
		t.Fatalf("miss_wait: plain %v, osv %v", p.MissWait, o.MissWait)
	}
	if o.TTL != 24*time.Hour || o.Attempts != 1 || o.ValidateJSON || o.MaxBodyBytes != 1<<20 {
		t.Fatalf("osv: %+v", o)
	}
	if got := o.Limiter.Limit(); got < 1.6 || got > 1.7 {
		t.Fatalf("rate 50/30s = %v", got)
	}
	if v, _ := o.AuthValue(); v != "k1" || o.AuthHeader != "apiKey" {
		t.Fatalf("auth: %q", v)
	}
}

func TestBuildReportsBadRates(t *testing.T) {
	c := load(t, map[string]string{"SOURCES": "a,b", "A_URL": "https://a.example", "A_RATE": "fast", "B_URL": "https://b.example", "B_RATE": "0/1s"})
	_, err := Build(c)
	if err == nil || !strings.Contains(err.Error(), "sources.a: rate") || !strings.Contains(err.Error(), "sources.b: rate") {
		t.Fatalf("err = %v", err)
	}
}

func TestFetch(t *testing.T) {
	var calls atomic.Int32
	var status atomic.Int32
	status.Store(http.StatusServiceUnavailable)
	keyFile := filepath.Join(t.TempDir(), "key")
	srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if r.Header.Get("apiKey") != "secret" || r.URL.RawQuery != "a=1&noRejected" || r.URL.Path != "/base/rest/json/cves/2.0" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if n == 1 {
			w.WriteHeader(int(status.Load()))
			return
		}
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))

	if err := os.WriteFile(keyFile, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := srv.Client() // also sets srv.URL
	srcs, err := Build(load(t, map[string]string{
		"SOURCES": "nvd", "NVD_URL": srv.URL + "/base", "NVD_AUTH_HEADER": "apiKey", "NVD_AUTH_VALUE_FILE": keyFile,
	}))
	if err != nil {
		t.Fatal(err)
	}
	s := srcs["nvd"]
	s.Client, s.RetryDelay = client, time.Millisecond
	r := NewRequest("nvd", "GET", "/rest/json/cves/2.0", "a=1&noRejected", "", nil)

	resp, err := s.Fetch(context.Background(), r)
	if err != nil || gunzip(t, resp.Gzip) != `{"ok":true}` || resp.Size != 11 || calls.Load() != 2 {
		t.Fatalf("retry on 503: %v calls=%d", err, calls.Load())
	}

	// The key file is re-read on every request.
	if err := os.WriteFile(keyFile, []byte("rotated"), 0o600); err != nil {
		t.Fatal(err)
	}
	calls.Store(1)
	_, err = s.Fetch(context.Background(), r)
	if code, ok := ClientError(err); !ok || code != http.StatusBadRequest || calls.Load() != 2 {
		t.Fatalf("4xx must not be retried: %v calls=%d", err, calls.Load())
	}
}

func gunzip(t *testing.T, b []byte) string {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}
