package raft

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

// TCPTransport dials peer raft addresses for RequestVote and AppendEntries RPCs.
type TCPTransport struct {
	peerAddrs map[string]string
	timeout   time.Duration
}

// NewTCPTransport creates a network transport using node-id to raft-address mapping.
func NewTCPTransport(peerAddrs map[string]string) *TCPTransport {
	return &TCPTransport{
		peerAddrs: peerAddrs,
		timeout:   500 * time.Millisecond,
	}
}

func (t *TCPTransport) RequestVote(peerID string, req RequestVoteRequest) (RequestVoteResponse, error) {
	var resp RequestVoteResponse
	if err := t.call(peerID, rpcRequestVote, req, &resp); err != nil {
		return RequestVoteResponse{}, err
	}
	return resp, nil
}

func (t *TCPTransport) AppendEntries(peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	var resp AppendEntriesResponse
	if err := t.call(peerID, rpcAppendEntries, req, &resp); err != nil {
		return AppendEntriesResponse{}, err
	}
	return resp, nil
}

func (t *TCPTransport) call(peerID, method string, req, resp any) error {
	addr, ok := t.peerAddrs[peerID]
	if !ok {
		return fmt.Errorf("unknown peer %q", peerID)
	}

	conn, err := net.DialTimeout("tcp", addr, t.timeout)
	if err != nil {
		return fmt.Errorf("dial peer %s: %w", peerID, err)
	}
	defer conn.Close()

	if err := writeRPC(conn, method, req); err != nil {
		return err
	}

	reader := bufio.NewReader(conn)
	gotMethod, err := readRPC(reader, resp)
	if err != nil {
		return err
	}
	if gotMethod != method {
		return fmt.Errorf("unexpected rpc method %q", gotMethod)
	}
	return nil
}
