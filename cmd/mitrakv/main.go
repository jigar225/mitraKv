package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jigar/mitrakv/internal/metrics"
	"github.com/jigar/mitrakv/internal/server"
	"github.com/jigar/mitrakv/internal/store"
	"github.com/jigar/mitrakv/internal/wal"
)

func main() {
	port := flag.Int("port", 6379, "TCP port to listen on")
	metricsPort := flag.Int("metrics-port", 9090, "HTTP port exposing /metrics")
	dataDir := flag.String("data-dir", "./data/node1", "Directory for durable WAL data")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	addr := fmtAddr(*port)
	metricsAddr := fmtAddr(*metricsPort)
	st := store.New()
	if err := wal.Replay(*dataDir, st); err != nil {
		slog.Error("failed to replay wal", "err", err, "data_dir", *dataDir)
		os.Exit(1)
	}

	writeAheadLog, err := wal.Open(*dataDir)
	if err != nil {
		slog.Error("failed to open wal", "err", err, "data_dir", *dataDir)
		os.Exit(1)
	}
	defer func() {
		if closeErr := writeAheadLog.Close(); closeErr != nil {
			slog.Warn("failed to close wal", "err", closeErr)
		}
	}()

	recorder, err := metrics.NewRecorder()
	if err != nil {
		slog.Error("failed to initialize metrics", "err", err)
		os.Exit(1)
	}
	srv := server.New(addr, st, recorder, writeAheadLog)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go runMetricsServer(ctx, metricsAddr, recorder.Handler())

	slog.Info("starting mitrakv", "addr", addr, "data_dir", *dataDir)
	if err := srv.ListenAndServe(ctx); err != nil {
		slog.Error("tcp server stopped", "err", err)
		os.Exit(1)
	}
}

func fmtAddr(port int) string {
	return fmt.Sprintf(":%d", port)
}

func runMetricsServer(ctx context.Context, addr string, handler http.Handler) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", handler)

	httpSrv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			slog.Warn("metrics server shutdown failed", "err", err)
		}
	}()

	slog.Info("metrics server listening", "addr", addr, "path", "/metrics")
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("metrics server stopped", "err", err)
	}
}
