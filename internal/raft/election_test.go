package raft

import (
	"context"
	"testing"
	"time"
)

func TestHandleRequestVoteGrantsVoteOncePerTerm(t *testing.T) {
	voter := NewNode(Config{NodeID: "node2", Peers: []string{"node1", "node2", "node3"}})

	req := RequestVoteRequest{
		Term:         1,
		CandidateID:  "node1",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp := voter.HandleRequestVote(req)
	if !resp.VoteGranted {
		t.Fatal("first vote should be granted")
	}

	other := RequestVoteRequest{
		Term:         1,
		CandidateID:  "node3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	resp = voter.HandleRequestVote(other)
	if resp.VoteGranted {
		t.Fatal("second vote in same term should be rejected")
	}
}

func TestHandleRequestVoteRejectsStaleTerm(t *testing.T) {
	voter := NewNode(Config{NodeID: "node2", Peers: []string{"node1", "node2", "node3"}})
	voter.mu.Lock()
	voter.currentTerm = 5
	voter.mu.Unlock()

	resp := voter.HandleRequestVote(RequestVoteRequest{
		Term:        3,
		CandidateID: "node1",
	})
	if resp.VoteGranted {
		t.Fatal("stale term should not receive vote")
	}
}

func TestStartElectionWinsMajority(t *testing.T) {
	peers := []string{"node1", "node2", "node3"}
	nodes := map[string]*Node{
		"node1": NewNode(Config{NodeID: "node1", Peers: peers, ElectionTimeoutMin: 50 * time.Millisecond, ElectionTimeoutMax: 50 * time.Millisecond}),
		"node2": NewNode(Config{NodeID: "node2", Peers: peers, ElectionTimeoutMin: 50 * time.Millisecond, ElectionTimeoutMax: 50 * time.Millisecond}),
		"node3": NewNode(Config{NodeID: "node3", Peers: peers, ElectionTimeoutMin: 50 * time.Millisecond, ElectionTimeoutMax: 50 * time.Millisecond}),
	}
	transport := newClusterTransport(nodes)
	for id, node := range nodes {
		node.SetTransport(transport)
		_ = id
	}

	if !nodes["node1"].startElection() {
		t.Fatal("node1 should win election with majority")
	}
	if !nodes["node1"].IsLeader() {
		t.Fatal("node1 should become leader")
	}
}

func TestRunElectsLeaderWithoutHeartbeats(t *testing.T) {
	peers := []string{"node1", "node2", "node3"}
	nodes := map[string]*Node{
		"node1": NewNode(Config{NodeID: "node1", Peers: peers, ElectionTimeoutMin: 30 * time.Millisecond, ElectionTimeoutMax: 40 * time.Millisecond}),
		"node2": NewNode(Config{NodeID: "node2", Peers: peers, ElectionTimeoutMin: 30 * time.Millisecond, ElectionTimeoutMax: 40 * time.Millisecond}),
		"node3": NewNode(Config{NodeID: "node3", Peers: peers, ElectionTimeoutMin: 30 * time.Millisecond, ElectionTimeoutMax: 40 * time.Millisecond}),
	}
	transport := newClusterTransport(nodes)
	for _, node := range nodes {
		node.SetTransport(transport)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, node := range nodes {
		go func(n *Node) {
			_ = n.Run(ctx)
		}(node)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		leaders := 0
		for _, node := range nodes {
			if node.IsLeader() {
				leaders++
			}
		}
		if leaders == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("expected exactly one leader to be elected")
}

func TestNotifyHeartbeatPreventsElection(t *testing.T) {
	node := NewNode(Config{
		NodeID:             "node2",
		Peers:              []string{"node1", "node2", "node3"},
		ElectionTimeoutMin: 80 * time.Millisecond,
		ElectionTimeoutMax: 80 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = node.Run(ctx)
	}()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	deadline := time.After(250 * time.Millisecond)
	for {
		select {
		case <-deadline:
			if node.IsLeader() {
				t.Fatal("follower receiving heartbeats should not become leader")
			}
			return
		case <-ticker.C:
			node.NotifyHeartbeat(1, "node1")
		}
	}
}
