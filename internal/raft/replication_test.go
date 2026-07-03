package raft

import (
	"context"
	"testing"
	"time"

	"github.com/jigar/mitrakv/internal/store"
)

func applyCommand(st *store.Store, cmd Command) {
	switch cmd.Op {
	case OpSet:
		st.Set(cmd.Key, cmd.Value)
	case OpDel:
		st.Del(cmd.Key)
	}
}

func setupReplicationCluster(t *testing.T) (map[string]*Node, map[string]*store.Store) {
	t.Helper()

	peers := []string{"node1", "node2", "node3"}
	baseCfg := Config{
		Peers:              peers,
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 80 * time.Millisecond,
		ElectionTimeoutMax: 120 * time.Millisecond,
	}
	nodes := map[string]*Node{
		"node1": NewNode(Config{NodeID: "node1", Peers: peers, HeartbeatInterval: baseCfg.HeartbeatInterval, ElectionTimeoutMin: baseCfg.ElectionTimeoutMin, ElectionTimeoutMax: baseCfg.ElectionTimeoutMax}),
		"node2": NewNode(Config{NodeID: "node2", Peers: peers, HeartbeatInterval: baseCfg.HeartbeatInterval, ElectionTimeoutMin: baseCfg.ElectionTimeoutMin, ElectionTimeoutMax: baseCfg.ElectionTimeoutMax}),
		"node3": NewNode(Config{NodeID: "node3", Peers: peers, HeartbeatInterval: baseCfg.HeartbeatInterval, ElectionTimeoutMin: baseCfg.ElectionTimeoutMin, ElectionTimeoutMax: baseCfg.ElectionTimeoutMax}),
	}
	stores := map[string]*store.Store{
		"node1": store.New(),
		"node2": store.New(),
		"node3": store.New(),
	}

	transport := newClusterTransport(nodes)
	for id, node := range nodes {
		node.SetTransport(transport)
		node.SetApplier(func(cmd Command) {
			applyCommand(stores[id], cmd)
		})
	}

	return nodes, stores
}

func waitForLeader(t *testing.T, nodes map[string]*Node) *Node {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node.IsLeader() {
				return node
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return nil
}

func TestProposeReplicatesToMajority(t *testing.T) {
	nodes, stores := setupReplicationCluster(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, node := range nodes {
		go func(n *Node) {
			_ = n.Run(ctx)
		}(node)
	}

	leader := waitForLeader(t, nodes)
	if err := leader.Propose(Command{Op: OpSet, Key: "city", Value: "mumbai"}); err != nil {
		t.Fatalf("propose: %v", err)
	}

	for id, st := range stores {
		val, ok := st.Get("city")
		if !ok || val != "mumbai" {
			t.Fatalf("store %s city = %q, %v; want mumbai, true", id, val, ok)
		}
	}
}

func TestProposeRejectedOnFollower(t *testing.T) {
	node := NewNode(Config{NodeID: "node2", Peers: []string{"node1", "node2", "node3"}})
	if err := node.Propose(Command{Op: OpSet, Key: "k", Value: "v"}); err != ErrNotLeader {
		t.Fatalf("Propose err = %v, want ErrNotLeader", err)
	}
}

func TestHandleAppendEntriesRejectsMissingPrevEntry(t *testing.T) {
	follower := NewNode(Config{NodeID: "node2", Peers: []string{"node1", "node2", "node3"}})

	resp := follower.HandleAppendEntries(AppendEntriesRequest{
		Term:         1,
		LeaderID:     "node1",
		PrevLogIndex: 5,
		PrevLogTerm:  1,
	})
	if resp.Success {
		t.Fatal("AppendEntries should fail when prev log entry is missing")
	}
}

func TestLeaderHeartbeatPreventsElection(t *testing.T) {
	nodes, _ := setupReplicationCluster(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, node := range nodes {
		go func(n *Node) {
			_ = n.Run(ctx)
		}(node)
	}

	leader := waitForLeader(t, nodes)
	_ = leader

	deadline := time.After(400 * time.Millisecond)
	ticker := time.NewTicker(30 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			leaders := 0
			for _, node := range nodes {
				if node.IsLeader() {
					leaders++
				}
			}
			if leaders != 1 {
				t.Fatalf("leader count = %d, want 1", leaders)
			}
			return
		case <-ticker.C:
			// heartbeats flow via Run() on leader
		}
	}
}
