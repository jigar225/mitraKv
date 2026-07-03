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

	"github.com/jigar/mitrakv/internal/config"
	"github.com/jigar/mitrakv/internal/metrics"
	"github.com/jigar/mitrakv/internal/raft"
	"github.com/jigar/mitrakv/internal/server"
	"github.com/jigar/mitrakv/internal/store"
	"github.com/jigar/mitrakv/internal/wal"
)

func main() {
	configPath := flag.String("config", "", "Path to cluster config YAML")
	port := flag.Int("port", 6379, "TCP port to listen on")
	metricsPort := flag.Int("metrics-port", 9090, "HTTP port exposing /metrics")
	raftPort := flag.Int("raft-port", 0, "Raft RPC port (cluster mode)")
	dataDir := flag.String("data-dir", "./data/node1", "Directory for durable WAL data")
	nodeID := flag.String("node-id", "", "Raft node id (cluster mode)")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *configPath != "" {
		runCluster(ctx, *configPath)
		return
	}

	runStandalone(ctx, standaloneOpts{
		port:        *port,
		metricsPort: *metricsPort,
		raftPort:    *raftPort,
		dataDir:     *dataDir,
		nodeID:      *nodeID,
	})
}

type standaloneOpts struct {
	port        int
	metricsPort int
	raftPort    int
	dataDir     string
	nodeID      string
}

func runStandalone(ctx context.Context, opts standaloneOpts) {
	st, writeAheadLog := openStore(opts.dataDir)
	defer closeWAL(writeAheadLog)

	recorder := mustRecorder()
	var raftNode *raft.Node

	if opts.raftPort > 0 && opts.nodeID != "" {
		raftNode = startRaftNode(ctx, opts.nodeID, []string{opts.nodeID}, map[string]string{
			opts.nodeID: fmtAddr(opts.raftPort),
		}, st, writeAheadLog, opts.raftPort)
	} else {
		raftNode = nil
	}

	srv := server.New(fmtAddr(opts.port), st, recorder, writeAheadLog, raftNode)
	go runMetricsServer(ctx, fmtAddr(opts.metricsPort), recorder.Handler())

	slog.Info("starting mitrakv", "addr", fmtAddr(opts.port), "data_dir", opts.dataDir, "raft", raftNode != nil)
	if err := srv.ListenAndServe(ctx); err != nil {
		slog.Error("tcp server stopped", "err", err)
		os.Exit(1)
	}
}

func runCluster(ctx context.Context, path string) {
	cfg, err := config.Load(path)
	if err != nil {
		slog.Error("failed to load config", "err", err, "path", path)
		os.Exit(1)
	}

	st, writeAheadLog := openStore(cfg.DataDir)
	defer closeWAL(writeAheadLog)

	raftNode := startRaftNode(ctx, cfg.NodeID, cfg.PeerIDs, cfg.PeerAddrs, st, writeAheadLog, cfg.RaftPort)
	recorder := mustRecorder()
	srv := server.New(fmtAddr(cfg.ClientPort), st, recorder, writeAheadLog, raftNode)

	go runMetricsServer(ctx, fmtAddr(cfg.MetricsPort), recorder.Handler())

	slog.Info("starting mitrakv cluster node",
		"node_id", cfg.NodeID,
		"client", fmtAddr(cfg.ClientPort),
		"raft", fmtAddr(cfg.RaftPort),
		"data_dir", cfg.DataDir,
	)
	if err := srv.ListenAndServe(ctx); err != nil {
		slog.Error("tcp server stopped", "err", err)
		os.Exit(1)
	}
}

func openStore(dataDir string) (*store.Store, *wal.Log) {
	st := store.New()
	if err := wal.Replay(dataDir, st); err != nil {
		slog.Error("failed to replay wal", "err", err, "data_dir", dataDir)
		os.Exit(1)
	}

	writeAheadLog, err := wal.Open(dataDir)
	if err != nil {
		slog.Error("failed to open wal", "err", err, "data_dir", dataDir)
		os.Exit(1)
	}
	return st, writeAheadLog
}

func closeWAL(writeAheadLog *wal.Log) {
	if writeAheadLog == nil {
		return
	}
	if err := writeAheadLog.Close(); err != nil {
		slog.Warn("failed to close wal", "err", err)
	}
}

func mustRecorder() *metrics.Recorder {
	recorder, err := metrics.NewRecorder()
	if err != nil {
		slog.Error("failed to initialize metrics", "err", err)
		os.Exit(1)
	}
	return recorder
}

func startRaftNode(
	ctx context.Context,
	nodeID string,
	peerIDs []string,
	peerAddrs map[string]string,
	st *store.Store,
	writeAheadLog *wal.Log,
	raftPort int,
) *raft.Node {
	node := raft.NewNode(raft.Config{NodeID: nodeID, Peers: peerIDs})
	node.SetTransport(raft.NewTCPTransport(peerAddrs))
	node.SetApplier(func(cmd raft.Command) {
		applyCommand(writeAheadLog, st, cmd)
	})

	rpcSrv := raft.NewRPCServer(fmtAddr(raftPort), node)
	go func() {
		if err := rpcSrv.ListenAndServe(ctx); err != nil {
			slog.Error("raft rpc server stopped", "err", err)
		}
	}()
	go func() {
		if err := node.Run(ctx); err != nil {
			slog.Error("raft node stopped", "err", err)
		}
	}()

	return node
}

func applyCommand(writeAheadLog *wal.Log, st *store.Store, cmd raft.Command) {
	switch cmd.Op {
	case raft.OpSet:
		if err := writeAheadLog.AppendSet(cmd.Key, cmd.Value); err != nil {
			slog.Error("wal append set failed during raft apply", "err", err, "key", cmd.Key)
			return
		}
		st.Set(cmd.Key, cmd.Value)
	case raft.OpDel:
		if err := writeAheadLog.AppendDel(cmd.Key); err != nil {
			slog.Error("wal append del failed during raft apply", "err", err, "key", cmd.Key)
			return
		}
		st.Del(cmd.Key)
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
