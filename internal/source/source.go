// Package source describes upstream HTTP APIs and fetches from them.
package source

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/metrics"
)

// UserAgent is set by main to include the version.
var UserAgent = "grafana-infinity-datasource-cache"

type Source struct {
	Name         string
	URL          *url.URL // request path is appended to it
	Paths        []string // allowed path prefixes
	Methods      []string
	TTL          time.Duration
	Timeout      time.Duration // per attempt
	MissWait     time.Duration // how long a cold request waits, 0 = the cache default
	Attempts     int
	RetryDelay   time.Duration
	Concurrency  int           // keys refreshed in parallel by the background pass
	MaxKeys      int           // tracked keys besides warmup, 0 = unlimited
	Limiter      *rate.Limiter // nil = unlimited
	Pending      chan struct{} // bounds active and queued fetches, including detached cache misses
	Slots        chan struct{} // upstream requests of this source in flight; nil = unlimited
	AuthHeader   string
	AuthValue    func() (string, error)
	ValidateJSON bool
	MaxBodyBytes int64
	Client       *http.Client
}

// Response is an upstream body already gzipped: it is never held uncompressed (see consume).
type Response struct {
	ContentType string
	Gzip        []byte
	Size        int64  // uncompressed
	Hash        string // sha256 of the uncompressed body
}

// Error is an upstream failure. Status is the upstream HTTP code, 0 when there was no response.
type Error struct {
	Status    int
	Msg       string
	Temporary bool
	Reason    string // label of upstream_errors_total
}

func (e *Error) Error() string { return e.Msg }

// Timeout reports an upstream that did not answer in time: no response within the attempt timeout, or no
// free slot because the source's requests hang. Clients get 504, not 502.
func Timeout(err error) bool {
	e, ok := errors.AsType[*Error](err)
	return ok && (e.Reason == "timeout" || e.Reason == "slots")
}

// ClientError reports an upstream 4xx (except 429): the query itself is wrong, pass the code through.
func ClientError(err error) (int, bool) {
	if e, ok := errors.AsType[*Error](err); ok && e.Status >= 400 && e.Status < 500 && e.Status != http.StatusTooManyRequests {
		return e.Status, true
	}
	return 0, false
}

// Allows is the only thing between a client and the upstream with our credentials attached, so a prefix
// matches whole segments ("/cves" is not "/cves-admin") and the path must not climb out of it.
func (s *Source) Allows(method, path string) bool {
	if !slices.Contains(s.Methods, method) || !SafePath(path) {
		return false
	}
	for _, p := range s.Paths {
		if path == p || strings.HasPrefix(path, p) && (strings.HasSuffix(p, "/") || path[len(p)] == '/') {
			return true
		}
	}
	return false
}

// SafePath rejects decoded paths an upstream or a proxy in front of it may resolve outside the allowed
// prefix: "/cves/%2e%2e/admin" arrives here as "/cves/../admin", and the HTTP client sends it as is.
// Rejected: "." and ".." segments (also "..;" as Tomcat reads it), empty segments, backslashes,
// control characters.
func SafePath(p string) bool {
	if !strings.HasPrefix(p, "/") || strings.ContainsFunc(p, func(r rune) bool { return r == '\\' || r < 0x20 || r == 0x7f }) {
		return false
	}
	segs := strings.Split(p[1:], "/")
	for i, seg := range segs {
		seg, _, _ = strings.Cut(seg, ";")
		if seg == "." || seg == ".." || seg == "" && i != len(segs)-1 {
			return false
		}
	}
	return true
}

// MaxFetchTime bounds Fetch with all retries; locks must outlive it.
func (s *Source) MaxFetchTime() time.Duration {
	n := time.Duration(max(s.Attempts, 1))
	return n*s.Timeout + n*n*s.RetryDelay
}

func (s *Source) Fetch(ctx context.Context, r Request) (*Response, error) {
	if !s.Allows(r.Method, r.Path) {
		return nil, &Error{Status: http.StatusForbidden, Msg: "request is no longer allowed for source", Reason: "not_allowed"}
	}
	if s.Pending != nil {
		select {
		case s.Pending <- struct{}{}:
			defer func() { <-s.Pending }()
		default:
			return nil, &Error{Status: http.StatusServiceUnavailable, Msg: "source fetch queue is full", Reason: "overloaded"}
		}
	}
	var last error
	for attempt := 1; attempt <= max(s.Attempts, 1); attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				return nil, last
			case <-time.After(time.Duration(attempt-1) * s.RetryDelay):
			}
			metrics.UpstreamRetries.WithLabelValues(s.Name).Inc()
		}
		resp, err := s.fetchOnce(ctx, r)
		if err == nil {
			return resp, nil
		}
		last = err
		if e, ok := errors.AsType[*Error](err); !ok || !e.Temporary {
			break
		}
	}
	return nil, last
}

func (s *Source) target(r Request) string {
	u := s.URL.Clone()
	u.Path = strings.TrimSuffix(u.Path, "/") + r.Path
	u.RawPath = ""
	u.RawQuery = r.Query
	return u.String()
}

func (s *Source) fetchOnce(ctx context.Context, r Request) (*Response, error) {
	resp, err := s.attempt(ctx, r)
	if err != nil {
		reason := "other"
		if e, ok := errors.AsType[*Error](err); ok && e.Reason != "" {
			reason = e.Reason
		}
		metrics.UpstreamErrors.WithLabelValues(s.Name, reason).Inc()
	}
	return resp, err
}

func (s *Source) attempt(ctx context.Context, r Request) (*Response, error) {
	if s.Limiter != nil {
		start := time.Now()
		err := s.Limiter.Wait(ctx)
		metrics.UpstreamRateLimitWait.WithLabelValues(s.Name).Observe(time.Since(start).Seconds())
		if err != nil {
			return nil, &Error{Msg: "rate limiter: " + err.Error(), Reason: "rate_limiter"}
		}
	}
	if s.Slots != nil {
		start := time.Now()
		select {
		case s.Slots <- struct{}{}:
		case <-ctx.Done():
			return nil, &Error{Msg: "waiting for an upstream slot: " + ctx.Err().Error(), Reason: "slots"}
		}
		defer func() { <-s.Slots }()
		metrics.UpstreamSlotWait.WithLabelValues(s.Name).Observe(time.Since(start).Seconds())
	}
	ctx, cancel := context.WithTimeout(ctx, s.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, r.Method, s.target(r), bytes.NewReader(r.Body))
	if err != nil {
		return nil, &Error{Msg: err.Error()}
	}
	req.Header.Set("Accept", "application/json, */*;q=0.5")
	req.Header.Set("User-Agent", UserAgent)
	if r.ContentType != "" {
		req.Header.Set("Content-Type", r.ContentType)
	}
	if s.AuthHeader != "" && s.AuthValue != nil {
		v, err := s.AuthValue()
		if err != nil {
			return nil, &Error{Msg: "read auth secret: " + err.Error(), Reason: "auth_secret"}
		}
		if v != "" {
			req.Header.Set(s.AuthHeader, v)
		}
	}

	inFlight := metrics.UpstreamInFlight.WithLabelValues(s.Name)
	inFlight.Inc()
	defer inFlight.Dec()
	start := time.Now()
	defer func() { metrics.UpstreamDuration.WithLabelValues(s.Name).Observe(time.Since(start).Seconds()) }()

	resp, err := s.Client.Do(req)
	if err != nil {
		metrics.UpstreamRequests.WithLabelValues(s.Name, "error").Inc()
		return nil, &Error{Msg: err.Error(), Temporary: true, Reason: NetworkReason(err)}
	}
	defer func() { _ = resp.Body.Close() }()
	metrics.UpstreamRequests.WithLabelValues(s.Name, strconv.Itoa(resp.StatusCode)).Inc()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		head, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return nil, &Error{
			Status:    resp.StatusCode,
			Msg:       fmt.Sprintf("upstream returned %d: %s", resp.StatusCode, snippet(head)),
			Temporary: resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500,
			Reason:    statusReason(resp.StatusCode),
		}
	}
	out, cerr := s.consume(resp.Body)
	if cerr != nil {
		cerr.Status = resp.StatusCode
		return nil, cerr
	}
	metrics.UpstreamResponseSize.WithLabelValues(s.Name).Observe(float64(out.Size))
	out.ContentType = cmp.Or(resp.Header.Get("Content-Type"), "application/json")
	return out, nil
}

// consume validates, gzips and hashes the body in one streaming pass. Holding it whole would cost
// three copies of up to UPSTREAM_MAX_BODY per fetch (read buffer growth, validation, compression):
// four parallel 64 MiB feeds took over 500 MiB RSS. Streaming costs about the compressed size.
func (s *Source) consume(body io.Reader) (*Response, *Error) {
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	sum := sha256.New()
	head := &headWriter{}
	rd := &recordingReader{r: io.LimitReader(body, s.MaxBodyBytes+1)}
	tee := io.TeeReader(rd, io.MultiWriter(zw, sum, head))

	var bad error
	if s.ValidateJSON {
		// A 200 with an HTML maintenance page must not replace good data. Lenient like json.Valid:
		// duplicate names and invalid UTF-8 are upstream's business, not a reason to drop the data.
		dec := jsontext.NewDecoder(tee, jsontext.AllowDuplicateNames(true), jsontext.AllowInvalidUTF8(true))
		if bad = dec.SkipValue(); bad == nil {
			if _, err := dec.ReadToken(); !errors.Is(err, io.EOF) {
				bad = errors.New("data after the top-level value")
			}
		}
	} else {
		_, bad = io.Copy(io.Discard, tee)
	}
	switch {
	case rd.err != nil && !errors.Is(rd.err, io.EOF):
		// Shodan drops connections mid-body: such errors land here, not in Do.
		return nil, &Error{Msg: "read upstream body: " + rd.err.Error(), Temporary: true, Reason: NetworkReason(rd.err)}
	case rd.n > s.MaxBodyBytes:
		return nil, &Error{Msg: fmt.Sprintf("upstream body exceeds %d bytes", s.MaxBodyBytes), Reason: "too_large"}
	case bad != nil:
		return nil, &Error{Msg: "upstream returned invalid JSON: " + snippet(head.b), Temporary: true, Reason: "invalid_json"}
	}
	if err := zw.Close(); err != nil {
		return nil, &Error{Msg: "gzip: " + err.Error()}
	}
	return &Response{Gzip: gz.Bytes(), Size: rd.n, Hash: hex.EncodeToString(sum.Sum(nil))}, nil
}

// recordingReader counts bytes and keeps the read error, which the JSON decoder would mix with syntax errors.
type recordingReader struct {
	r   io.Reader
	n   int64
	err error
}

func (r *recordingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	if err != nil {
		r.err = err
	}
	return n, err
}

// headWriter keeps the first bytes of the body for error messages.
type headWriter struct{ b []byte }

func (w *headWriter) Write(p []byte) (int, error) {
	if room := 256 - len(w.b); room > 0 {
		w.b = append(w.b, p[:min(room, len(p))]...)
	}
	return len(p), nil
}

func statusReason(code int) string {
	switch {
	case code == http.StatusTooManyRequests:
		return "http_429"
	case code >= 500:
		return "http_5xx"
	case code >= 400:
		return "http_4xx"
	case code >= 300:
		return "http_3xx" // a redirect the client refused to follow, see sameOrigin
	}
	return "other"
}

// NetworkReason classifies transport errors into a fixed set of metric label values.
func NetworkReason(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case wraps[*net.DNSError](err):
		return "dns"
	case wraps[*tls.CertificateVerificationError](err), wraps[tls.RecordHeaderError](err),
		wraps[x509.UnknownAuthorityError](err):
		return "tls"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused"
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return "reset"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "eof"
	}
	if ne, ok := errors.AsType[net.Error](err); ok && ne.Timeout() {
		return "timeout"
	}
	return "other"
}

// wraps reports whether err's chain holds an error of type T.
func wraps[T error](err error) bool {
	_, ok := errors.AsType[T](err)
	return ok
}

func snippet(b []byte) string {
	const n = 200
	s := strings.Join(strings.Fields(string(b)), " ")
	if len(s) > n {
		s = s[:n] + "…"
	}
	return s
}
