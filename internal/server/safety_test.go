package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/cache"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/metrics"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/source"
)

// An encoded dot segment is decoded before routing and would reach the upstream as "/cves/../admin"
// with the source credentials; such paths must never leave the service.
func TestUnsafePathNeverReachesUpstream(t *testing.T) {
	var calls atomic.Int32
	up := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, `{}`)
	}))
	client := up.Client()
	u, _ := url.Parse(up.URL)
	srcs := map[string]*source.Source{"up": {
		Name: "up", URL: u, Paths: []string{"/cves/"}, Methods: []string{"GET"},
		TTL: time.Hour, Timeout: time.Second, Attempts: 1, ValidateJSON: true, MaxBodyBytes: 1 << 20, Client: client,
	}}
	log := slog.New(slog.DiscardHandler)
	c := cache.New(cache.NewMemoryStore(), srcs, cache.Options{
		MaxStale: time.Hour, IdleTTL: time.Hour, RefreshInterval: time.Minute,
		ErrorBackoff: time.Minute, MissWait: 5 * time.Second, LocalCheck: time.Second, Owner: "t",
	}, log)
	t.Cleanup(func() { c.Close(time.Minute) })
	srv := httptest.NewTestServer(t, New(c, srcs, log))
	srv.Client()

	for _, p := range []string{
		"/up/cves/%2e%2e/admin",
		"/up/cves/..%2fadmin",
		"/up/cves/%2E%2E%2Fadmin",
		"/up/cves/..;/admin",
		"/up/cves/x%5c..%5cadmin",
		"/up/cves/x%2F%2Fy",
		"/up/cves/%2e/x",
		"/up/cves/x%00",
	} {
		req, _ := http.NewRequestWithContext(context.Background(), "GET", srv.URL+p, nil)
		resp, err := srv.Client().Transport.RoundTrip(req) // no redirects: a 301 from the mux would also be fine
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: code %d, want 400", p, resp.StatusCode)
		}
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("upstream got %d requests", n)
	}
	if r := get(t, srv, "/up/cves/ok", false); r.code != 200 {
		t.Fatalf("safe path: %+v", r)
	}
}

// A client that gives up (Infinity's timeout) is not a service failure: 499, not 5xx.
func TestClientGivingUpIs499(t *testing.T) {
	up := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	client := up.Client()
	u, _ := url.Parse(up.URL)
	srcs := map[string]*source.Source{"slow": {
		Name: "slow", URL: u, Paths: []string{"/"}, Methods: []string{"GET"},
		TTL: time.Hour, Timeout: 2 * time.Second, Attempts: 1, MaxBodyBytes: 1 << 20, Client: client,
	}}
	log := slog.New(slog.DiscardHandler)
	c := cache.New(cache.NewMemoryStore(), srcs, cache.Options{
		MaxStale: time.Hour, IdleTTL: time.Hour, RefreshInterval: time.Minute,
		ErrorBackoff: time.Minute, MissWait: 5 * time.Second, LocalCheck: time.Second, Owner: "t",
	}, log)
	t.Cleanup(func() { c.Close(time.Second) })
	srv := httptest.NewServer(New(c, srcs, log)) // real loopback: the in-memory network does not report client disconnects
	t.Cleanup(srv.Close)

	canceled := func() float64 { return testutil.ToFloat64(metrics.Requests.WithLabelValues("slow", "canceled", "499")) }
	bad := func() float64 { return testutil.ToFloat64(metrics.Requests.WithLabelValues("slow", "error", "502")) }
	c0, b0 := canceled(), bad()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/slow/x", nil)
	if resp, err := srv.Client().Do(req); err == nil {
		_ = resp.Body.Close()
		t.Fatal("want client timeout")
	}
	eventually := time.Now().Add(2 * time.Second)
	for canceled()-c0 != 1 && time.Now().Before(eventually) {
		time.Sleep(10 * time.Millisecond)
	}
	if canceled()-c0 != 1 || bad()-b0 != 0 {
		t.Fatalf("canceled +%v, 502 +%v", canceled()-c0, bad()-b0)
	}
}

// An upstream that does not answer in time is a gateway timeout, also while the failure is backed off.
func TestUpstreamTimeoutIs504(t *testing.T) {
	up := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	client := up.Client()
	u, _ := url.Parse(up.URL)
	srcs := map[string]*source.Source{"slow": {
		Name: "slow", URL: u, Paths: []string{"/"}, Methods: []string{"GET"},
		TTL: time.Hour, Timeout: 100 * time.Millisecond, Attempts: 1, MaxBodyBytes: 1 << 20, Client: client,
	}}
	log := slog.New(slog.DiscardHandler)
	c := cache.New(cache.NewMemoryStore(), srcs, cache.Options{
		MaxStale: time.Hour, IdleTTL: time.Hour, RefreshInterval: time.Minute,
		ErrorBackoff: time.Minute, MissWait: 5 * time.Second, LocalCheck: time.Second, Owner: "t",
	}, log)
	t.Cleanup(func() { c.Close(time.Second) })
	srv := httptest.NewTestServer(t, New(c, srcs, log))
	srv.Client()
	for _, label := range []string{"fetch", "backoff"} {
		if r := get(t, srv, "/slow/x", false); r.code != http.StatusGatewayTimeout {
			t.Fatalf("%s: code %d, want 504: %s", label, r.code, r.body)
		}
	}
}

func TestOverloadIs503(t *testing.T) {
	err := &source.Error{Status: http.StatusServiceUnavailable, Reason: "overloaded", Msg: "source fetch queue is full"}
	if got := errorCode(err); got != http.StatusServiceUnavailable {
		t.Fatalf("overload code = %d, want 503", got)
	}
}
