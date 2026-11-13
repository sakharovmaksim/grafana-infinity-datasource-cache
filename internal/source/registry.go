package source

import (
	"cmp"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/config"
)

const (
	defaultAttempts    = 2
	defaultConcurrency = 4
	retryDelay         = 3 * time.Second
)

// HTTP/2 multiplexes every request to a host over one connection, and Go never pings it by default: once
// the path silently drops packets (NAT, firewall failover), every request to that source times out on the
// same dead connection until the kernel gives up on it, tens of minutes later. A ping unanswered within
// h2PingAfter+h2PingTimeout closes it, and the retry dials a new one. Kept well below the shortest
// per-attempt timeout so the retry does not land on the dead connection. Variables for tests.
var (
	h2PingAfter   = 15 * time.Second
	h2PingTimeout = 10 * time.Second
)

// Build creates the enabled sources of c. Upstream specifics (URL, limits, auth) come only from the config.
func Build(c *config.Config) (map[string]*Source, error) {
	out := map[string]*Source{}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.HTTP2 = &http.HTTP2Config{SendPingTimeout: h2PingAfter, PingTimeout: h2PingTimeout}
	client := &http.Client{Transport: tr, CheckRedirect: sameOrigin}
	var errs []error
	for name, sc := range c.Sources {
		if !sc.IsEnabled() {
			continue
		}
		s, err := build(name, sc, c, client)
		if err != nil {
			errs = append(errs, fmt.Errorf("sources.%s: %w", name, err))
			continue
		}
		out[name] = s
	}
	return out, errors.Join(errs...)
}

func build(name string, sc *config.SourceConfig, c *config.Config, client *http.Client) (*Source, error) {
	u, err := url.Parse(sc.URL)
	if err != nil {
		return nil, err
	}
	limiter, err := parseRate(sc.Rate)
	if err != nil {
		return nil, fmt.Errorf("rate: %w", err)
	}
	s := &Source{
		Name:        name,
		URL:         u,
		Paths:       or(sc.Paths, []string{"/"}),
		Methods:     upper(or(sc.Methods, []string{http.MethodGet})),
		TTL:         cmp.Or(sc.TTL, c.Cache.TTL).D(),
		Timeout:     cmp.Or(sc.Timeout, c.Upstream.Timeout).D(),
		MissWait:    cmp.Or(sc.MissWait, c.Cache.MissWait).D(),
		Attempts:    cmp.Or(sc.Attempts, defaultAttempts),
		Concurrency: cmp.Or(sc.Concurrency, defaultConcurrency),
		MaxKeys:     cmp.Or(sc.MaxKeys, c.Cache.MaxKeys),
		// Per source: a shared pool would let one hanging source hold every slot and stall the others.
		Slots:        make(chan struct{}, max(cmp.Or(sc.MaxInFlight, c.Upstream.MaxInFlight), 1)),
		Pending:      make(chan struct{}, 2*max(cmp.Or(sc.MaxInFlight, c.Upstream.MaxInFlight), 1)),
		RetryDelay:   retryDelay,
		Limiter:      limiter,
		AuthHeader:   sc.Auth.Header,
		AuthValue:    config.SecretGetter(sc.Auth.Value, sc.Auth.ValueFile),
		ValidateJSON: sc.Validate != "none",
		MaxBodyBytes: int64(cmp.Or(sc.MaxBody, c.Upstream.MaxBody)),
		Client:       client,
	}
	// Share the transport, but enforce each source's path and method policy on redirects.
	ownClient := *client
	ownClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if err := sameOrigin(req, via); err != nil {
			return err
		}
		base := strings.TrimSuffix(s.URL.Path, "/")
		if !strings.HasPrefix(req.URL.Path, base+"/") || !s.Allows(req.Method, strings.TrimPrefix(req.URL.Path, base)) {
			return http.ErrUseLastResponse
		}
		return nil
	}
	s.Client = &ownClient
	return s, nil
}

// sameOrigin follows redirects only within the scheme and host of the request. Go drops Authorization on a
// redirect to another host but keeps custom headers, so an auth.header like apiKey would go wherever the
// upstream (or anyone who controls a path on it) points, including plain http.
func sameOrigin(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}
	if req.URL.Scheme != via[0].URL.Scheme || req.URL.Host != via[0].URL.Host {
		return http.ErrUseLastResponse
	}
	return nil
}

// parseRate reads "50/30s": at most 50 requests per 30 seconds, evenly spaced.
func parseRate(s string) (*rate.Limiter, error) {
	if s == "" || s == "0" {
		return nil, nil
	}
	n, per, ok := strings.Cut(s, "/")
	count, err := strconv.Atoi(n)
	if !ok || err != nil || count <= 0 {
		return nil, fmt.Errorf("want <count>/<duration>, got %q", s)
	}
	window, err := config.ParseDuration(per)
	if err != nil || window <= 0 {
		return nil, fmt.Errorf("want <count>/<duration>, got %q", s)
	}
	return rate.NewLimiter(rate.Every(window/time.Duration(count)), 1), nil
}

func or(v, def []string) []string {
	if len(v) > 0 {
		return v
	}
	return def
}

func upper(v []string) []string {
	out := make([]string, len(v))
	for i, s := range v {
		out[i] = strings.ToUpper(s)
	}
	return out
}
