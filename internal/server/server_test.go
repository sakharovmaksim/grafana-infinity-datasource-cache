package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/cache"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/metrics"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/source"
)

// handler is the service in front of an in-memory upstream answering status and body.
func handler(t *testing.T, status int, body string) http.Handler {
	t.Helper()
	up := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	client := up.Client() // also sets up.URL
	u, _ := url.Parse(up.URL)
	srcs := map[string]*source.Source{"up": {
		Name: "up", URL: u, Paths: []string{"/cves"}, Methods: []string{"GET"},
		TTL: time.Hour, Timeout: time.Second, Attempts: 1, ValidateJSON: true,
		MaxBodyBytes: 1 << 20, Client: client,
	}}
	log := slog.New(slog.DiscardHandler)
	c := cache.New(cache.NewMemoryStore(), srcs, cache.Options{
		MaxStale: time.Hour, IdleTTL: time.Hour, RefreshInterval: time.Minute,
		ErrorBackoff: time.Minute, MissWait: 5 * time.Second, LocalCheck: time.Second, Owner: "t",
	}, log)
	t.Cleanup(func() { c.Close(time.Minute) })
	return New(c, srcs, log)
}

// setup serves the service on the in-memory network; requests go through srv.Client().
func setup(t *testing.T, status int, body string) *httptest.Server {
	srv := httptest.NewTestServer(t, handler(t, status, body))
	srv.Client()
	return srv
}

type result struct {
	code         int
	header       http.Header
	body         string
	uncompressed bool
}

func get(t *testing.T, srv *httptest.Server, path string, gzip bool) result {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), "GET", srv.URL+path, nil)
	if !gzip {
		req.Header.Set("Accept-Encoding", "identity")
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return result{resp.StatusCode, resp.Header, string(b), resp.Uncompressed}
}

func TestProxy(t *testing.T) {
	srv := setup(t, http.StatusOK, `{"cves":[]}`)

	r := get(t, srv, "/up/cves?product=go", false)
	if r.code != 200 || r.body != `{"cves":[]}` || r.header.Get("X-Cache") != "MISS" {
		t.Fatalf("identity: %+v", r)
	}
	// Go's client asks for gzip and decodes transparently, as Grafana does.
	r = get(t, srv, "/up/cves?product=go", true)
	if r.body != `{"cves":[]}` || r.header.Get("X-Cache") != "HIT" || !r.uncompressed {
		t.Fatalf("gzip: %+v", r)
	}

	for path, code := range map[string]int{"/nope/cves": 404, "/up/other": 404} {
		if r := get(t, srv, path, false); r.code != code {
			t.Errorf("%s: %d", path, r.code)
		}
	}
	if r := get(t, srv, "/-/entries", false); r.code != 200 || !strings.Contains(r.body, `"state":"fresh"`) {
		t.Errorf("entries: %+v", r)
	}
}

func TestUpstreamErrors(t *testing.T) {
	if r := get(t, setup(t, http.StatusNotFound, `{}`), "/up/cves", false); r.code != 404 {
		t.Errorf("4xx passthrough: %d", r.code)
	}
	if r := get(t, setup(t, http.StatusServiceUnavailable, `{}`), "/up/cves", false); r.code != 502 {
		t.Errorf("5xx: %d", r.code)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	srv := setup(t, http.StatusOK, `{"cves":[]}`)
	get(t, srv, "/up/cves?product=go", false)
	get(t, srv, "/up/cves?product=go", true)
	get(t, srv, "/unknown/x", false)

	r := get(t, srv, "/metrics", false)
	for _, want := range []string{
		`datasource_cache_requests_total{code="200",result="hit",source="up"}`,
		`datasource_cache_requests_total{code="200",result="miss",source="up"}`,
		`datasource_cache_rejected_requests_total{reason="unknown_source"}`,
		`datasource_cache_response_bytes_total{encoding="gzip",source="up"}`,
		`datasource_cache_served_age_seconds_bucket{result="hit",source="up",le="60"}`,
		`datasource_cache_upstream_response_size_bytes_count{source="up"}`,
		`go_sched_latencies_seconds_bucket`,
		`promhttp_metric_handler_requests_total`,
	} {
		if !strings.Contains(r.body, want) {
			t.Errorf("/metrics has no %s", want)
		}
	}

	problems, err := testutil.GatherAndLint(metrics.Registry)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		if strings.HasPrefix(p.Metric, "datasource_cache_") {
			t.Errorf("lint %s: %s", p.Metric, p.Text)
		}
	}
}

// The write deadline set for one response must not cut the next response on the same keep-alive connection.
func TestWriteDeadlineDoesNotLeakAcrossKeepAlive(t *testing.T) {
	old := writeTimeout
	writeTimeout = 100 * time.Millisecond
	defer func() { writeTimeout = old }()
	// Deadlines are a property of real sockets: listen on loopback instead of the in-memory network.
	srv := httptest.NewTestServer(t, handler(t, http.StatusOK, `{"cves":[]}`))
	srv.Start()

	var reused []bool
	client := &http.Client{Transport: &http.Transport{MaxConnsPerHost: 1}}
	do := func(path string) int {
		ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
			GotConn: func(i httptrace.GotConnInfo) { reused = append(reused, i.Reused) },
		})
		req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+path, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		defer func() { _ = resp.Body.Close() }()
		_, _ = io.ReadAll(resp.Body)
		return resp.StatusCode
	}
	if code := do("/up/cves"); code != 200 {
		t.Fatalf("first: %d", code)
	}
	time.Sleep(300 * time.Millisecond)
	if code := do("/nope/x"); code != 404 {
		t.Fatalf("error response after the deadline: %d", code)
	}
	time.Sleep(300 * time.Millisecond)
	if code := do("/metrics"); code != 200 {
		t.Fatalf("metrics after the deadline: %d", code)
	}
	if len(reused) != 3 || !reused[1] || !reused[2] {
		t.Fatalf("connection not reused, test proves nothing: %v", reused)
	}
}

func TestHeadServedAsGet(t *testing.T) {
	srv := setup(t, http.StatusOK, `{"cves":[]}`)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodHead, srv.URL+"/up/cves?product=go", nil)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("X-Cache") != "MISS" || resp.ContentLength != int64(len(`{"cves":[]}`)) {
		t.Fatalf("HEAD: %d %v len=%d", resp.StatusCode, resp.Header, resp.ContentLength)
	}
	// HEAD filled the same key as GET.
	if r := get(t, srv, "/up/cves?product=go", false); r.header.Get("X-Cache") != "HIT" || r.body != `{"cves":[]}` {
		t.Fatalf("GET after HEAD: %+v", r)
	}
}

// Check both wire semantics and cached responses, including empty POST bodies.
func TestProxyKeepsRequestVariantsSeparate(t *testing.T) {
	up := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, r.Header.Get("Content-Type")+"\n"+r.URL.RawQuery+"\n"+string(body))
	}))
	client := up.Client()
	u, _ := url.Parse(up.URL)
	srcs := map[string]*source.Source{"up": {
		Name: "up", URL: u, Paths: []string{"/query"}, Methods: []string{"POST"},
		TTL: time.Hour, Timeout: time.Second, Attempts: 1, MaxBodyBytes: 1 << 20, Client: client,
	}}
	log := slog.New(slog.DiscardHandler)
	c := cache.New(cache.NewMemoryStore(), srcs, cache.Options{
		MaxStale: time.Hour, IdleTTL: time.Hour, MissWait: 5 * time.Second, LocalCheck: time.Minute, Owner: "test",
	}, log)
	t.Cleanup(func() { c.Close(time.Second) })
	srv := httptest.NewTestServer(t, New(c, srcs, log))
	srv.Client()
	cases := []struct{ contentType, query, body string }{
		{"application/json", "", "{}"},
		{"text/plain", "", "{}"},
		{"application/json", "", ""},
		{"text/plain", "", ""},
		{"text/plain", "flag", ""},
		{"text/plain", "flag=", ""},
		{"text/plain", "sort=desc&sort=asc", ""},
		{"text/plain", "sort=asc&sort=desc", ""},
		{"text/plain", "sort=desc&%73ort=asc&flag&&x=cpe:2.3", ""},
	}
	for _, wantCache := range []string{"MISS", "HIT"} {
		for _, tt := range cases {
			req, err := http.NewRequestWithContext(context.Background(), "POST", srv.URL+"/up/query?"+tt.query, strings.NewReader(tt.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", tt.contentType)
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			want := tt.contentType + "\n" + tt.query + "\n" + tt.body
			if resp.StatusCode != 200 || string(body) != want || resp.Header.Get("X-Cache") != wantCache {
				t.Fatalf("%+v: status=%d body=%q cache=%s; want body=%q cache=%s", tt, resp.StatusCode, body, resp.Header.Get("X-Cache"), want, wantCache)
			}
		}
	}
}
