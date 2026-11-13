package source

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/metrics"
)

func TestAllowsStaysInsidePrefix(t *testing.T) {
	s := &Source{Methods: []string{"GET"}, Paths: []string{"/cves", "/cve/", "/rest/json/"}}
	for path, want := range map[string]bool{
		"/cves":                true,
		"/cves/":               true,
		"/cves/x":              true,
		"/cve/CVE-2024-1":      true,
		"/rest/json/cves/2.0":  true,
		"/cves-admin":          false, // prefix matches whole segments
		"/cvesx":               false,
		"/cve":                 false, // "/cve/" asks for something below it
		"/cves/../admin":       false,
		"/cves/..":             false,
		"/cves/./x":            false,
		"/cves/..;/admin":      false, // Tomcat drops ";..." from a segment
		"/cves//x":             false,
		"/cves/x\\..\\admin":   false,
		"/cves/x\x00":          false,
		"/rest/json/../../etc": false,
	} {
		if got := s.Allows("GET", path); got != want {
			t.Errorf("Allows(%q) = %v, want %v", path, got, want)
		}
	}
	if !(&Source{Methods: []string{"GET"}, Paths: []string{"/"}}).Allows("GET", "/anything/below") {
		t.Error(`"/" must allow everything safe`)
	}
}

// Go drops Authorization on a cross-host redirect but keeps custom headers: such redirects are not followed.
func TestRedirectStaysOnOrigin(t *testing.T) {
	var leaked atomic.Value
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked.Store(r.Header.Get("apiKey"))
		_, _ = io.WriteString(w, `{}`)
	}))
	defer other.Close()
	otherURL := strings.Replace(other.URL, "127.0.0.1", "localhost", 1) // same server, another host
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/away":
			http.Redirect(w, r, otherURL+"/x", http.StatusFound)
		case "/moved":
			http.Redirect(w, r, "/here", http.StatusMovedPermanently)
		default:
			if r.Header.Get("apiKey") != "secret" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_, _ = io.WriteString(w, `{"ok":true}`)
		}
	}))
	defer up.Close()

	srcs, err := Build(load(t, map[string]string{
		"SOURCES": "up", "UP_URL": up.URL, "UP_AUTH_HEADER": "apiKey", "UP_AUTH_VALUE": "secret", "UP_ATTEMPTS": "1",
	}))
	if err != nil {
		t.Fatal(err)
	}
	s := srcs["up"]
	if _, err := s.Fetch(context.Background(), NewRequest("up", "GET", "/moved", "", "", nil)); err != nil {
		t.Fatalf("same-origin redirect must be followed with the key: %v", err)
	}
	before := testutil.ToFloat64(metrics.UpstreamErrors.WithLabelValues("up", "http_3xx"))
	_, err = s.Fetch(context.Background(), NewRequest("up", "GET", "/away", "", "", nil))
	if e, ok := errors.AsType[*Error](err); !ok || e.Status != http.StatusFound {
		t.Fatalf("cross-host redirect: err = %v", err)
	}
	if v := leaked.Load(); v != nil {
		t.Fatalf("the other host was called, apiKey = %q", v)
	}
	if d := testutil.ToFloat64(metrics.UpstreamErrors.WithLabelValues("up", "http_3xx")) - before; d != 1 {
		t.Fatalf("http_3xx errors += %v", d)
	}
}

func TestSlotsBoundUpstreamRequests(t *testing.T) {
	var cur, peak atomic.Int32
	srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := cur.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(20 * time.Millisecond)
		cur.Add(-1)
		_, _ = io.WriteString(w, `{}`)
	}))
	client := srv.Client()
	u, _ := url.Parse(srv.URL)
	slots := make(chan struct{}, 2)
	var wg sync.WaitGroup
	for i := range 6 {
		s := &Source{Name: "s", URL: u, Methods: []string{"GET"}, Paths: []string{"/"}, Timeout: 5 * time.Second,
			Attempts: 1, MaxBodyBytes: 1 << 20, Client: client, Slots: slots}
		wg.Go(func() {
			if _, err := s.Fetch(context.Background(), NewRequest("s", "GET", "/", "i="+string(rune('a'+i)), "", nil)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if p := peak.Load(); p != 2 {
		t.Fatalf("peak upstream requests = %d, want 2", p)
	}
}

func TestRedirectRespectsSourcePolicy(t *testing.T) {
	var forbidden atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/base/read/escape":
			http.Redirect(w, r, "/base/admin", http.StatusFound)
		case "/base/read/outside":
			http.Redirect(w, r, "/read/ok", http.StatusFound)
		case "/base/read/move":
			http.Redirect(w, r, "/base/read/ok", http.StatusFound)
		case "/base/read/ok":
			_, _ = io.WriteString(w, `{}`)
		default:
			forbidden.Add(1)
			_, _ = io.WriteString(w, `{}`)
		}
	}))
	defer up.Close()
	srcs, err := Build(load(t, map[string]string{
		"SOURCES": "up", "UP_URL": up.URL + "/base", "UP_PATHS": "/read/", "UP_ATTEMPTS": "1",
		"UP_AUTH_HEADER": "apiKey", "UP_AUTH_VALUE": "secret",
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/read/escape", "/read/outside"} {
		_, err := srcs["up"].Fetch(context.Background(), NewRequest("up", "GET", path, "", "", nil))
		if e, ok := errors.AsType[*Error](err); !ok || e.Status != http.StatusFound {
			t.Fatalf("%s: expected rejected redirect, got %v", path, err)
		}
	}
	if _, err := srcs["up"].Fetch(context.Background(), NewRequest("up", "GET", "/read/move", "", "", nil)); err != nil {
		t.Fatal(err)
	}
	srcs["up"].Methods = []string{"POST"}
	_, err = srcs["up"].Fetch(context.Background(), NewRequest("up", "POST", "/read/move", "", "", nil))
	if e, ok := errors.AsType[*Error](err); !ok || e.Status != http.StatusFound {
		t.Fatalf("redirect must not change POST to disallowed GET: %v", err)
	}
	if forbidden.Load() != 0 {
		t.Fatal("redirect reached forbidden endpoint")
	}
}

func TestFetchRechecksPolicy(t *testing.T) {
	s := &Source{Paths: []string{"/read/"}, Methods: []string{"GET"}}
	for _, r := range []Request{
		NewRequest("up", "GET", "/admin", "", "", nil),
		NewRequest("up", "POST", "/read/query", "", "", nil),
	} {
		_, err := s.Fetch(context.Background(), r)
		if e, ok := errors.AsType[*Error](err); !ok || e.Status != http.StatusForbidden {
			t.Fatalf("want policy rejection, got %v", err)
		}
	}
}

func TestFetchQueueBounded(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer up.Close()
	defer close(release)
	srcs, err := Build(load(t, map[string]string{"SOURCES": "up", "UP_URL": up.URL, "UP_MAX_IN_FLIGHT": "1", "UP_ATTEMPTS": "1"}))
	if err != nil {
		t.Fatal(err)
	}
	s := srcs["up"]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	defer wg.Wait()
	// Cancel before waiting for the workers on failure, too.
	defer cancel()
	for range 2 {
		wg.Go(func() { _, _ = s.Fetch(ctx, NewRequest("up", "GET", "/", "", "", nil)) })
	}
	<-started
	deadline := time.Now().Add(time.Second)
	for len(s.Pending) != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(s.Pending) != 2 {
		t.Fatal("queue did not fill")
	}
	_, err = s.Fetch(ctx, NewRequest("up", "GET", "/extra", "", "", nil))
	if e, ok := errors.AsType[*Error](err); !ok || e.Reason != "overloaded" {
		t.Fatalf("want immediate overload rejection, got %v", err)
	}
}
