package source

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/metrics"
)

func TestNetworkReason(t *testing.T) {
	wrap := func(err error) error { return &url.Error{Op: "Get", URL: "https://x", Err: err} }
	for want, err := range map[string]error{
		"timeout":  wrap(context.DeadlineExceeded),
		"canceled": wrap(context.Canceled),
		"dns":      wrap(&net.OpError{Op: "dial", Err: &net.DNSError{Err: "no such host", Name: "x"}}),
		"tls":      wrap(x509.UnknownAuthorityError{}),
		"refused":  wrap(&net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}),
		"reset":    wrap(&net.OpError{Op: "read", Err: os.NewSyscallError("read", syscall.ECONNRESET)}),
		"eof":      wrap(io.EOF),
		"other":    errors.New("boom"),
	} {
		if got := NetworkReason(err); got != want {
			t.Errorf("%v: got %s, want %s", err, got, want)
		}
	}
}

// Cloudflare-style drop: the connection closes without a response.
func TestUpstreamErrorMetrics(t *testing.T) {
	srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/drop":
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close()
		case "/slow":
			time.Sleep(300 * time.Millisecond)
		case "/html":
			_, _ = io.WriteString(w, "<html>maintenance</html>")
		default:
			w.WriteHeader(http.StatusTooManyRequests)
		}
	}))
	client := srv.Client() // also sets srv.URL
	u, _ := url.Parse(srv.URL)
	s := &Source{Name: "m", URL: u, Paths: []string{"/"}, Methods: []string{"GET"}, Timeout: 100 * time.Millisecond, Attempts: 2, RetryDelay: time.Millisecond,
		ValidateJSON: true, MaxBodyBytes: 1 << 20, Client: client}

	for path, reason := range map[string]string{"/drop": "eof", "/slow": "timeout", "/html": "invalid_json", "/429": "http_429"} {
		get := func() float64 { return testutil.ToFloat64(metrics.UpstreamErrors.WithLabelValues("m", reason)) }
		retries := testutil.ToFloat64(metrics.UpstreamRetries.WithLabelValues("m"))
		before := get()
		_, err := s.Fetch(context.Background(), NewRequest("m", "GET", path, "", "", nil))
		if err == nil {
			t.Fatalf("%s: want error", path)
		}
		// Every reason above is temporary, so each path makes two attempts.
		if d := get() - before; d != 2 {
			t.Errorf("%s: upstream_errors{reason=%s} delta = %v (%v)", path, reason, d, err)
		}
		if d := testutil.ToFloat64(metrics.UpstreamRetries.WithLabelValues("m")) - retries; d != 1 {
			t.Errorf("%s: retries delta = %v", path, d)
		}
	}
	if v := testutil.ToFloat64(metrics.UpstreamInFlight.WithLabelValues("m")); v != 0 {
		t.Errorf("in-flight gauge leaked: %v", v)
	}
}
