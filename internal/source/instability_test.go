package source

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blackhole is a TCP proxy whose existing connections can be made to drop all packets while staying
// open, as a NAT or firewall that lost its state does; new connections go through.
type blackhole struct {
	ln       net.Listener
	accepted atomic.Int32
	mu       sync.Mutex
	dead     []*atomic.Bool
}

func newBlackhole(t *testing.T, target string) *blackhole {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &blackhole{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			b.accepted.Add(1)
			s, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", target)
			if err != nil {
				_ = c.Close()
				continue
			}
			dead := &atomic.Bool{}
			b.mu.Lock()
			b.dead = append(b.dead, dead)
			b.mu.Unlock()
			pump := func(dst, src net.Conn) {
				buf := make([]byte, 32<<10)
				for {
					n, err := src.Read(buf)
					if err != nil {
						if !dead.Load() {
							_ = dst.Close()
						}
						return
					}
					if !dead.Load() {
						if _, err := dst.Write(buf[:n]); err != nil {
							return
						}
					}
				}
			}
			go pump(s, c)
			go pump(c, s)
		}
	}()
	return b
}

func (b *blackhole) killExisting() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, d := range b.dead {
		d.Store(true)
	}
}

// Without HTTP/2 pings every later request to the source reused the dead connection and timed out.
func TestDeadHTTP2ConnectionIsReplaced(t *testing.T) {
	defer func(a, b time.Duration) { h2PingAfter, h2PingTimeout = a, b }(h2PingAfter, h2PingTimeout)
	h2PingAfter, h2PingTimeout = 200*time.Millisecond, 200*time.Millisecond

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"proto":"`+r.Proto+`"}`)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	bh := newBlackhole(t, srv.Listener.Addr().String())
	srcs, err := Build(load(t, map[string]string{"SOURCES": "up", "UP_URL": "https://" + bh.ln.Addr().String(), "UP_TIMEOUT": "5s"}))
	if err != nil {
		t.Fatal(err)
	}
	s := srcs["up"]
	s.RetryDelay = 10 * time.Millisecond
	s.Client.Transport.(*http.Transport).TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	fetch := func() (string, error) {
		r, err := s.Fetch(context.Background(), NewRequest("up", "GET", "/x", "", "", nil))
		if err != nil {
			return "", err
		}
		return gunzip(t, r.Gzip), nil
	}

	if body, err := fetch(); err != nil || body != `{"proto":"HTTP/2.0"}` {
		t.Fatalf("warm: %q %v", body, err)
	}
	bh.killExisting()
	start := time.Now()
	if _, err := fetch(); err != nil {
		t.Fatalf("after the connection died: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second || bh.accepted.Load() != 2 {
		t.Fatalf("took %v, connections %d: the dead one was not replaced by the retry", d, bh.accepted.Load())
	}
}

// One source that hangs must not hold upstream slots of another.
func TestHangingSourceDoesNotStallOthers(t *testing.T) {
	srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hang" {
			<-r.Context().Done()
			return
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	client := srv.Client()
	srcs, err := Build(load(t, map[string]string{"SOURCES": "bad,good", "BAD_URL": srv.URL, "GOOD_URL": srv.URL,
		"BAD_TIMEOUT": "1s", "BAD_ATTEMPTS": "1", "UPSTREAM_MAX_IN_FLIGHT": "2", "GOOD_MAX_IN_FLIGHT": "3"}))
	if err != nil {
		t.Fatal(err)
	}
	if cap(srcs["bad"].Slots) != 2 || cap(srcs["good"].Slots) != 3 {
		t.Fatalf("slots: bad %d, good %d", cap(srcs["bad"].Slots), cap(srcs["good"].Slots))
	}
	srcs["bad"].Client, srcs["good"].Client = client, client
	var wg sync.WaitGroup
	defer wg.Wait()
	for i := range 4 { // more cold keys than the hanging source has slots
		wg.Go(func() {
			_, _ = srcs["bad"].Fetch(context.Background(), NewRequest("bad", "GET", "/hang", "i="+string(rune('a'+i)), "", nil))
		})
	}
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	if _, err := srcs["good"].Fetch(context.Background(), NewRequest("good", "GET", "/ok", "", "", nil)); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("healthy source waited %v", d)
	}
}
