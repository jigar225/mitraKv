package raft

import (
	"sync"
)

// clusterTransport routes RPCs between in-memory nodes for unit and integration tests.
type clusterTransport struct {
	mu    sync.Mutex
	nodes map[string]*Node
}

func newClusterTransport(nodes map[string]*Node) *clusterTransport {
	return &clusterTransport{nodes: nodes}
}

func (c *clusterTransport) RequestVote(peerID string, req RequestVoteRequest) (RequestVoteResponse, error) {
	c.mu.Lock()
	peer := c.nodes[peerID]
	c.mu.Unlock()
	if peer == nil {
		return RequestVoteResponse{VoteGranted: false}, nil
	}
	return peer.HandleRequestVote(req), nil
}

func (c *clusterTransport) AppendEntries(peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	c.mu.Lock()
	peer := c.nodes[peerID]
	c.mu.Unlock()
	if peer == nil {
		return AppendEntriesResponse{Success: false}, nil
	}
	return peer.HandleAppendEntries(req), nil
}
