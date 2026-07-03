package raft

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestTCPTransportRequestVote(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	voter := NewNode(Config{NodeID: "node2", Peers: []string{"node1", "node2"}})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		server := &RPCServer{node: voter}
		server.serveConn(conn)
	}()

	transport := NewTCPTransport(map[string]string{"node2": ln.Addr().String()})
	resp, err := transport.RequestVote("node2", RequestVoteRequest{
		Term:         1,
		CandidateID:  "node1",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !resp.VoteGranted {
		t.Fatal("expected vote to be granted")
	}
}

func TestRPCServerLifecycle(t *testing.T) {
	node := NewNode(Config{NodeID: "node1", Peers: []string{"node1"}})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	srv := NewRPCServer(addr, node)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("ListenAndServe: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("rpc server did not stop")
	}
}
