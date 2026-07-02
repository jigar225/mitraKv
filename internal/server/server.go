package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/jigar/mitrakv/internal/metrics"
	"github.com/jigar/mitrakv/internal/store"
	"github.com/jigar/mitrakv/internal/wal"
)

// Server accepts TCP connections and serves the MitraKV wire protocol.
type Server struct {
	addr    string
	store   *store.Store
	metrics *metrics.Recorder
	wal     *wal.Log
}

// New creates a server bound to addr (e.g. ":6379").
func New(addr string, st *store.Store, recorder *metrics.Recorder, writeAheadLog *wal.Log) *Server {
	return &Server{
		addr:    addr,
		store:   st,
		metrics: recorder,
		wal:     writeAheadLog,
	}
}

// ListenAndServe blocks until ctx is cancelled, then shuts down.
func (s *Server) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	defer ln.Close()

	slog.Info("mitrakv listening", "addr", ln.Addr().String())

	var wg sync.WaitGroup
	defer wg.Wait()

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return fmt.Errorf("accept: %w", err)
			}
		}

		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer c.Close()
			slog.Info("client connected", "remote", c.RemoteAddr().String())
			s.handleConn(ctx, c)
			slog.Info("client disconnected", "remote", c.RemoteAddr().String())
		}(conn)
	}
}
