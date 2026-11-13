package source

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

// consume must accept exactly what json.Valid accepts: the cache used it before streaming.
func TestConsumeMatchesJSONValid(t *testing.T) {
	s := &Source{ValidateJSON: true, MaxBodyBytes: 1 << 20}
	for _, body := range []string{
		`{"a":1}`, `[1,2,3]`, ` {"a":[{"b":null}]} ` + "\n", `"str"`, `42`, `{"a":1,"a":2}`, "{\"a\":\"\xff\"}",
		`<html>maintenance</html>`, `{"a":1} x`, `{}{}`, `{"a":`, ``, `{"a":1,}`,
	} {
		_, err := s.consume(strings.NewReader(body))
		if want := json.Valid([]byte(body)); (err == nil) != want {
			t.Errorf("%q: consume err=%v, json.Valid=%v", body, err, want)
		}
	}
}

func TestConsumeOutput(t *testing.T) {
	body := `{"cves":[` + strings.Repeat(`{"id":"CVE-2026-0001","score":9.8},`, 10000) + `{}]}`
	out, err := (&Source{ValidateJSON: true, MaxBodyBytes: 1 << 20}).consume(strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte(body))
	if gunzip(t, out.Gzip) != body || out.Size != int64(len(body)) || out.Hash != hex.EncodeToString(sum[:]) {
		t.Fatalf("size=%d hash=%s", out.Size, out.Hash)
	}
	if len(out.Gzip) > len(body)/10 {
		t.Fatalf("not compressed: %d of %d", len(out.Gzip), len(body))
	}
}

func TestConsumeErrors(t *testing.T) {
	strict := &Source{ValidateJSON: true, MaxBodyBytes: 16}
	if _, err := strict.consume(strings.NewReader(`{"a":"0123456789abcdef"}`)); err == nil || err.Reason != "too_large" {
		t.Errorf("too large: %v", err)
	}
	if _, err := strict.consume(strings.NewReader(`<html>x</html>`)); err == nil || err.Reason != "invalid_json" ||
		!strings.Contains(err.Msg, "<html>") || !err.Temporary {
		t.Errorf("invalid: %+v", err)
	}
	// A dropped connection is a network error, not invalid JSON.
	dropped := io.MultiReader(strings.NewReader(`{"a":`), errReader{io.ErrUnexpectedEOF})
	if _, err := (&Source{ValidateJSON: true, MaxBodyBytes: 1 << 20}).consume(dropped); err == nil || err.Reason != "eof" {
		t.Errorf("dropped: %+v", err)
	}
	if out, err := (&Source{MaxBodyBytes: 1 << 20}).consume(strings.NewReader(`not json`)); err != nil || out.Size != 8 {
		t.Errorf("_VALIDATE=none: %v", err)
	}
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }
