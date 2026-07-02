package server

import (
	"context"
	"testing"
	"time"

	"github.com/jigar/mitrakv/internal/metrics"
	"github.com/jigar/mitrakv/internal/store"
	"github.com/jigar/mitrakv/internal/wal"
)

// TestServerRestartRecovery simulates process restart: boot, write via TCP, shutdown,
// replay WAL into a fresh store, boot again, and confirm GET still returns the value.
func TestServerRestartRecovery(t *testing.T) {
	dataDir := t.TempDir()
	addr := reserveTCPAddress(t)

	shutdown := bootServer(t, addr, dataDir)
	assertResponse(t, addr, "SET city mumbai\n", "OK")
	shutdown()

	// Simulate a new process: replay WAL, reopen log, start server on same port.
	shutdown = bootServer(t, addr, dataDir)
	defer shutdown()

	assertResponse(t, addr, "GET city\n", "mumbai")
}

// bootServer replays WAL, opens a new log, and listens until cancel is called.
func bootServer(t *testing.T, addr, dataDir string) context.CancelFunc {
	t.Helper()

	st := store.New()
	if err := wal.Replay(dataDir, st); err != nil {
		t.Fatalf("replay wal: %v", err)
	}

	writeAheadLog, err := wal.Open(dataDir)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	recorder, err := metrics.NewRecorder()
	if err != nil {
		t.Fatalf("create metrics recorder: %v", err)
	}

	srv := New(addr, st, recorder, writeAheadLog)
	ctx, cancel := context.WithCancel(context.Background())

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.ListenAndServe(ctx)
	}()

	waitUntilListening(t, addr)

	return func() {
		cancel()

		select {
		case err := <-serverErr:
			if err != nil {
				t.Fatalf("server shutdown error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("server did not shut down in time")
		}

		if err := writeAheadLog.Close(); err != nil {
			t.Fatalf("close wal: %v", err)
		}
	}
}
