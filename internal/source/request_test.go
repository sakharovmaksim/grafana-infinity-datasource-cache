package source

import "testing"

func TestRequestKeyQuerySemantics(t *testing.T) {
	for _, tt := range []struct {
		name, a, b string
		equal      bool
	}{
		{"distinct names", "b=2&a=1", "a=1&b=2", true},
		{"percent encoding", "x=cpe:2.3:a&flag", "flag&x=cpe%3A2.3%3Aa", true},
		{"space encoding", "x=a+b", "x=a%20b", true},
		{"flag versus empty", "flag", "flag=", false},
		{"duplicate value order", "sort=desc&sort=asc", "sort=asc&sort=desc", false},
		{"encoded duplicate name", "sort=desc&%73ort=asc", "sort=desc&sort=asc", true},
		{"encoded reversed duplicates", "sort=desc&%73ort=asc", "sort=asc&sort=desc", false},
		{"interleaved duplicates", "sort=desc&x=1&sort=asc", "x=1&sort=desc&sort=asc", true},
		{"duplicate flag order", "flag&flag=", "flag=&flag", false},
		{"encoded delimiters", "x=a%26b%3Dc", "x=a&b=c", false},
		{"invalid value escape", "x=%zz", "x=%25zz", false},
		{"invalid name escape", "%zz=x", "%25zz=x", false},
		{"empty segment", "a=1&&b=2", "a=1&b=2", false},
		{"empty name versus absent query", "=", "", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := NewRequest("up", "GET", "/query", tt.a, "", nil)
			b := NewRequest("up", "GET", "/query", tt.b, "", nil)
			if a.Query != tt.a || b.Query != tt.b {
				t.Fatal("upstream query was rewritten")
			}
			if got := a.Key() == b.Key(); got != tt.equal {
				t.Fatalf("key equality = %v, want %v", got, tt.equal)
			}
		})
	}
}

func TestRequestContentTypeWithAndWithoutBody(t *testing.T) {
	for _, method := range []string{"GET", "POST"} {
		for _, body := range [][]byte{nil, []byte("{}")} {
			a := NewRequest("up", method, "/query", "", "application/json", body)
			b := NewRequest("up", method, "/query", "", "text/plain", body)
			if a.ContentType != "application/json" || b.ContentType != "text/plain" {
				t.Fatalf("%s body=%q: Content-Type lost", method, body)
			}
			if a.Key() == b.Key() {
				t.Fatalf("%s body=%q: Content-Type collision", method, body)
			}
		}
	}
}
