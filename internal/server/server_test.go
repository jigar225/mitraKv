package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jigar/mitrakv/internal/metrics"
	"github.com/jigar/mitrakv/internal/store"
)

// TestServerTCPFlow verifies why the TCP server contract matters end-to-end:
// it ensures protocol parsing, dispatch, and store mutation stay wired correctly.
func TestServerTCPFlow(t *testing.T) {
	addr := reserveTCPAddress(t)

	recorder, err := metrics.NewRecorder()
	if err != nil {
		t.Fatalf("create metrics recorder: %v", err)
	}

	srv := New(addr, store.New(), recorder, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.ListenAndServe(ctx)
	}()

	waitUntilListening(t, addr)

	assertResponse(t, addr, "PING\n", "PONG")
	assertResponse(t, addr, "SET name jigar\n", "OK")
	assertResponse(t, addr, "GET name\n", "jigar")
	assertResponse(t, addr, "DEL name\n", "OK")
	assertResponse(t, addr, "GET name\n", "(nil)")

	cancel()

	select {
	case err := <-serverErr:
		if err != nil {
			t.Fatalf("server shutdown error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not shut down in time")
	}
}

// reserveTCPAddress grabs an ephemeral TCP address so tests avoid hardcoded ports.
func reserveTCPAddress(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	defer ln.Close()

	return ln.Addr().String()
}

// waitUntilListening polls until the server accepts dials to avoid startup races.
func waitUntilListening(t *testing.T, addr string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	t.Fatalf("server did not start listening on %s", addr)
}

// assertResponse checks the wire-level request/response behavior over TCP.
func assertResponse(t *testing.T, addr, req, want string) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()

	if _, err := fmt.Fprint(conn, req); err != nil {
		t.Fatalf("write request %q: %v", req, err)
	}

	resp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("read response for %q: %v", req, err)
	}

	if got := strings.TrimSuffix(resp, "\n"); got != want {
		t.Fatalf("response for %q = %q, want %q", req, got, want)
	}
}
