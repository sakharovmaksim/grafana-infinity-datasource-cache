package source

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strings"
)

// Request identifies one cached upstream response and is stored so the refresher can replay it.
type Request struct {
	Source      string `json:"source"`
	Method      string `json:"method"`
	Path        string `json:"path"`
	Query       string `json:"query,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	Body        []byte `json:"body,omitempty"`
}

func NewRequest(source, method, path, rawQuery, contentType string, body []byte) Request {
	if path == "" {
		path = "/"
	}
	return Request{Source: source, Method: method, Path: path, Query: rawQuery, ContentType: contentType, Body: body}
}

// CanonicalQuery sorts by decoded parameter name only. Stable sorting preserves
// the order of repeated parameters, including differently encoded names.
// Raw pairs (including valueless flags and empty segments) are kept intact.
func CanonicalQuery(raw string) string {
	parts := strings.Split(raw, "&")
	name := func(pair string) string {
		k, _, _ := strings.Cut(pair, "=")
		if decoded, err := url.QueryUnescape(k); err == nil {
			return decoded
		}
		return k
	}
	sort.SliceStable(parts, func(i, j int) bool { return name(parts[i]) < name(parts[j]) })
	return strings.Join(parts, "&")
}

// Key includes every forwarded request field. Other client headers are not
// forwarded; credentials are supplied by the source and shared by its clients.
func (r Request) Key() string {
	h := sha256.New()
	// v2 must not reuse v1 entries: their query order or Content-Type was lost.
	_, _ = h.Write([]byte("datasource-cache-request-v2\n"))
	for _, field := range []string{r.Source, r.Method, r.Path, keyQuery(r.Query), r.ContentType} {
		_, _ = fmt.Fprintf(h, "%d:%s", len(field), field)
	}
	_, _ = h.Write(r.Body)
	return hex.EncodeToString(h.Sum(nil))[:32]
}

// keyQuery normalizes encoding and the order of distinct parameter names only.
// The upstream always receives the original query. Malformed escapes use a
// separate raw namespace so they cannot alias a valid encoded literal percent.
func keyQuery(q string) string {
	parts := strings.Split(q, "&")
	for i, p := range parts {
		k, v, hasEquals := strings.Cut(p, "=")
		key, keyErr := url.QueryUnescape(k)
		value, valueErr := url.QueryUnescape(v)
		if keyErr != nil || valueErr != nil {
			return "raw:" + q
		}
		parts[i] = url.QueryEscape(key)
		if hasEquals {
			parts[i] += "=" + url.QueryEscape(value)
		}
	}
	return "query:" + CanonicalQuery(strings.Join(parts, "&"))
}

// URL is the request as a client sends it to this service.
func (r Request) URL() string {
	u := "/" + r.Source + r.Path
	if r.Query != "" {
		u += "?" + r.Query
	}
	return u
}

func (r Request) String() string { return r.Method + " " + r.URL() }

// ParseTarget splits "/<source>/<path>?<query>" as used in CACHE_WARMUP.
func ParseTarget(target string) (name, path, query string, err error) {
	if !strings.HasPrefix(target, "/") {
		return "", "", "", fmt.Errorf("%q: must start with /<source>/", target)
	}
	target, query, _ = strings.Cut(target, "?")
	name, path, _ = strings.Cut(strings.TrimPrefix(target, "/"), "/")
	if name == "" {
		return "", "", "", fmt.Errorf("%q: no source name", target)
	}
	return name, "/" + path, query, nil
}
