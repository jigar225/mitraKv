package raft

import "testing"

func TestLogAppendAndLookup(t *testing.T) {
	log := NewLog()

	appended := log.Append(1, Command{Op: OpSet, Key: "name", Value: "jigar"})
	if len(appended) != 1 {
		t.Fatalf("appended len = %d, want 1", len(appended))
	}

	entry, ok := log.EntryAt(1)
	if !ok {
		t.Fatal("entry at index 1 should exist")
	}
	if entry.Term != 1 || entry.Command.Key != "name" {
		t.Fatalf("entry = %+v, want term=1 key=name", entry)
	}
	if log.LastIndex() != 1 || log.LastTerm() != 1 {
		t.Fatalf("last = index %d term %d, want 1/1", log.LastIndex(), log.LastTerm())
	}
}

func TestLogTruncateFrom(t *testing.T) {
	log := NewLog()
	log.Append(1,
		Command{Op: OpSet, Key: "a", Value: "1"},
		Command{Op: OpSet, Key: "b", Value: "2"},
		Command{Op: OpSet, Key: "c", Value: "3"},
	)

	if err := log.TruncateFrom(2); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if log.LastIndex() != 1 {
		t.Fatalf("last index = %d, want 1", log.LastIndex())
	}

	entries := log.EntriesFrom(2)
	if len(entries) != 0 {
		t.Fatalf("entries from 2 = %d, want 0", len(entries))
	}
}

func TestNewNodeStartsAsFollower(t *testing.T) {
	node := NewNode(Config{NodeID: "node1", Peers: []string{"node1", "node2", "node3"}})
	if node.State() != Follower {
		t.Fatalf("state = %s, want follower", node.State())
	}
	if node.CurrentTerm() != 0 {
		t.Fatalf("term = %d, want 0", node.CurrentTerm())
	}
}

func TestNodeRoleTransitions(t *testing.T) {
	node := NewNode(Config{NodeID: "node1", Peers: []string{"node1", "node2", "node3"}})

	node.mu.Lock()
	node.becomeCandidate()
	node.mu.Unlock()
	if node.State() != Candidate {
		t.Fatalf("state = %s, want candidate", node.State())
	}
	if node.CurrentTerm() != 1 {
		t.Fatalf("term = %d, want 1", node.CurrentTerm())
	}

	node.mu.Lock()
	node.becomeLeader()
	node.mu.Unlock()
	if !node.IsLeader() {
		t.Fatal("node should be leader")
	}
	if node.nextIndex["node2"] != 1 {
		t.Fatalf("nextIndex[node2] = %d, want 1", node.nextIndex["node2"])
	}
}
