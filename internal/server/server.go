// Package server exposes the cache over HTTP, mirroring upstream paths under /<source>/.
package server

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/cache"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/metrics"
	"github.com/sakharovmaksim/grafana-infinity-datasource-cache/internal/source"
)

const maxRequestBody = 1 << 20

// statusClientClosed (nginx's 499) marks requests the client gave up on: Infinity times out after 30–60 s,
// and counting that as 5xx would make the service look broken while the fetch goes on and fills the cache.
const statusClientClosed = 499

// writeTimeout: a client that stops reading must not pin a goroutine and a connection forever.
var writeTimeout = time.Minute

type Getter interface {
	Get(ctx context.Context, r source.Request) (*cache.Entry, cache.Status, error)
	Entries(ctx context.Context) ([]cache.EntryInfo, error)
}

type Server struct {
	cache   Getter
	sources map[string]*source.Source
	log     *slog.Logger
}

func New(c Getter, sources map[string]*source.Source, log *slog.Logger) http.Handler {
	s := &Server{cache: c, sources: sources, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /-/entries", s.entries)
	mux.HandleFunc("/{source}/{path...}", s.proxy)
	return mux
}

func (s *Server) proxy(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("source")
	src, ok := s.sources[name]
	if !ok {
		metrics.Rejected.WithLabelValues("unknown_source").Inc()
		writeError(w, http.StatusNotFound, "unknown source "+strconv.Quote(name))
		return
	}
	path := "/" + r.PathValue("path")
	// HEAD is a GET without the body: same key, the upstream never sees HEAD.
	method := r.Method
	if method == http.MethodHead {
		method = http.MethodGet
	}
	if !source.SafePath(path) {
		metrics.Rejected.WithLabelValues("unsafe_path").Inc()
		writeError(w, http.StatusBadRequest, "path must not contain dot or empty segments, backslashes or control characters")
		return
	}
	if !src.Allows(method, path) {
		metrics.Rejected.WithLabelValues("not_allowed").Inc()
		writeError(w, http.StatusNotFound, r.Method+" "+path+" is not allowed for source "+name)
		return
	}
	var body []byte
	if method != http.MethodGet {
		var err error
		if body, err = io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody)); err != nil {
			metrics.Rejected.WithLabelValues("body_too_large").Inc()
			writeError(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
	}
	req := source.NewRequest(name, method, path, r.URL.RawQuery, r.Header.Get("Content-Type"), body)

	inFlight := metrics.RequestsInFlight.WithLabelValues(name)
	inFlight.Inc()
	defer inFlight.Dec()
	start := time.Now()
	result, code := "error", http.StatusOK
	defer func() {
		metrics.Requests.WithLabelValues(name, result, strconv.Itoa(code)).Inc()
		metrics.RequestDuration.WithLabelValues(name, result).Observe(time.Since(start).Seconds())
	}()

	e, status, err := s.cache.Get(r.Context(), req)
	if err != nil && r.Context().Err() != nil {
		result, code = "canceled", statusClientClosed
		s.log.Debug("client gave up", "request", req.String(), "duration", time.Since(start))
		return
	}
	if err != nil {
		code = errorCode(err)
		s.log.Warn("request failed", "request", req.String(), "code", code, "error", err, "duration", time.Since(start))
		writeError(w, code, err.Error())
		return
	}
	result = strings.ToLower(string(status))
	age := time.Since(e.Meta.FetchedAt)
	metrics.ServedAge.WithLabelValues(name, result).Observe(age.Seconds())
	s.log.Debug("served", "request", req.String(), "cache", status, "duration", time.Since(start))

	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(writeTimeout))
	h := w.Header()
	h.Set("Content-Type", e.Meta.ContentType)
	h.Set("X-Cache", string(status))
	h.Set("Age", strconv.Itoa(int(age.Seconds())))
	h.Set("Last-Modified", e.Meta.FetchedAt.UTC().Format(http.TimeFormat))
	h.Set("Vary", "Accept-Encoding")
	if acceptsGzip(r) {
		h.Set("Content-Encoding", "gzip")
		h.Set("Content-Length", strconv.Itoa(len(e.Gzip)))
		if r.Method != http.MethodHead {
			n, _ := w.Write(e.Gzip)
			metrics.ResponseBytes.WithLabelValues(name, "gzip").Add(float64(n))
		}
		return
	}
	h.Set("Content-Length", strconv.FormatInt(e.Meta.Size, 10))
	if r.Method == http.MethodHead {
		return
	}
	zr, err := gzip.NewReader(bytes.NewReader(e.Gzip))
	if err != nil {
		s.log.Error("corrupt cache entry", "request", req.String(), "error", err)
		return
	}
	n, _ := io.Copy(w, zr)
	metrics.ResponseBytes.WithLabelValues(name, "identity").Add(float64(n))
}

func (s *Server) entries(w http.ResponseWriter, r *http.Request) {
	list, err := s.cache.Entries(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.MarshalWrite(w, list)
}

// errorCode passes upstream 4xx through (the query is wrong), maps waiting too long to 504 and the rest to 502.
func errorCode(err error) int {
	if e, ok := errors.AsType[*source.Error](err); ok && e.Reason == "overloaded" {
		return http.StatusServiceUnavailable
	}
	if code, ok := source.ClientError(err); ok {
		return code
	}
	if errors.Is(err, cache.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) || source.Timeout(err) {
		return http.StatusGatewayTimeout
	}
	return http.StatusBadGateway
}

func acceptsGzip(r *http.Request) bool {
	for part := range strings.SplitSeq(r.Header.Get("Accept-Encoding"), ",") {
		enc, q, _ := strings.Cut(strings.TrimSpace(part), ";")
		if strings.EqualFold(strings.TrimSpace(enc), "gzip") && strings.TrimSpace(q) != "q=0" {
			return true
		}
	}
	return false
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.MarshalWrite(w, map[string]string{"error": msg})
}
