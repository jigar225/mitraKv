package raft

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
)

// RPCServer accepts inbound Raft RPC calls from peer nodes.
type RPCServer struct {
	addr string
	node *Node
}

// NewRPCServer listens on addr and dispatches RPCs to the local Raft node.
func NewRPCServer(addr string, node *Node) *RPCServer {
	return &RPCServer{addr: addr, node: node}
}

// ListenAndServe blocks until ctx is cancelled.
func (s *RPCServer) ListenAndServe(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("raft rpc listen %s: %w", s.addr, err)
	}
	defer ln.Close()

	slog.Info("raft rpc listening", "addr", ln.Addr().String())

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
				return fmt.Errorf("raft accept: %w", err)
			}
		}

		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer c.Close()
			s.serveConn(c)
		}(conn)
	}
}

func (s *RPCServer) serveConn(conn net.Conn) {
	reader := bufio.NewReader(conn)

	method, err := reader.ReadString('\n')
	if err != nil {
		slog.Warn("raft rpc read method failed", "err", err)
		return
	}
	method = strings.TrimSpace(method)

	switch method {
	case rpcRequestVote:
		var req RequestVoteRequest
		if err := readRPCBody(reader, &req); err != nil {
			slog.Warn("raft rpc decode request vote failed", "err", err)
			return
		}
		resp := s.node.HandleRequestVote(req)
		if err := writeRPC(conn, rpcRequestVote, resp); err != nil {
			slog.Warn("raft rpc write request vote failed", "err", err)
		}
	case rpcAppendEntries:
		var req AppendEntriesRequest
		if err := readRPCBody(reader, &req); err != nil {
			slog.Warn("raft rpc decode append entries failed", "err", err)
			return
		}
		resp := s.node.HandleAppendEntries(req)
		if err := writeRPC(conn, rpcAppendEntries, resp); err != nil {
			slog.Warn("raft rpc write append entries failed", "err", err)
		}
	default:
		slog.Warn("raft rpc unknown method", "method", method)
	}
}

func readRPCBody(r *bufio.Reader, dest any) error {
	body, err := r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("read rpc body: %w", err)
	}
	body = strings.TrimSpace(body)
	if err := json.Unmarshal([]byte(body), dest); err != nil {
		return fmt.Errorf("decode rpc body: %w", err)
	}
	return nil
}
